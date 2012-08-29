package org.apache.hadoop.mapreduce.v2.app.release;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.speculate.ExponentiallySmoothedTaskRuntimeEstimator;
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

public class Releaser extends AbstractService implements
    EventHandler<ReleaseEvent> {
  
  public enum EventType {
    RELEASE_RESOURCES,
    RELEASED
  }

  private static final Log LOG = LogFactory.getLog(Releaser.class);
  
  private final Configuration conf;
  private final AppContext context;
  private Thread releaserBackgroundThread = null;
  private final int memory;
  private final EventHandler eventHandler;
  private final TaskRuntimeEstimator estimator;
  private final String releaseStrategy;
  private final int releaserPollInterval;
  
  private SortedMap<Long, TaskAttempt> suspendableTaskAttempts;
  
  public Releaser(Configuration conf, AppContext context, TaskRuntimeEstimator estimator) {
    super(Releaser.class.getName());
    this.conf = conf;
    this.context = context;
    
    this.memory = conf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
                              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    this.eventHandler = context.getEventHandler();
    this.estimator = estimator;
    this.releaseStrategy = conf.get(
        MRJobConfig.SUSPEND_STRATEGY, "none");
    this.releaserPollInterval = conf.getInt(
        MRJobConfig.SUSPEND_INTERVAL, 1000);
    
    LOG.info("(bcho2) release strategy "+this.releaseStrategy);
  }
  
  /*   *************************************************************    */

  // This is the task-mongering that creates the two new threads -- one for
  //  processing events from the event queue and one for periodically
  //  looking for speculation opportunities

  @Override
  public void start() {
    Runnable releaserBackgroundCore
        = new Runnable() {
            @Override
            public void run() {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  Thread.sleep(releaserPollInterval);
                  updateSuspendableTaskAttempts();
                } catch (InterruptedException e) {
                  LOG.error("Background thread returning, interrupted : " + e);
                  e.printStackTrace(System.out);
                  return;
                }
              }
            }
          };
    releaserBackgroundThread = new Thread
        (releaserBackgroundCore, "Releaser background processing");
    releaserBackgroundThread.start();

    super.start();
  }

  @Override
  public void stop() {
    // this could be called before background thread is established
    if (releaserBackgroundThread != null) {
      releaserBackgroundThread.interrupt();
    }
    super.stop();
  }

  private void updateSuspendableTaskAttempts() {
    LOG.info("(bcho2) updating suspendable task attempts start");
    SortedMap<Long, TaskAttempt> newSuspendableTaskAttempts =
      new TreeMap<Long, TaskAttempt>();
    for (Job job : context.getAllJobs().values()) {
      for (Task task : job.getTasks(TaskType.REDUCE).values()) {
        for (TaskAttempt ta : task.getAttempts().values()) {
          if (ta.getState() == TaskAttemptState.RUNNING) {
            long estimatedRuntime = estimator.estimatedRuntime(ta.getID());
            long estimatedEndtime = (estimatedRuntime > 0) ? 
                estimator.attemptEnrolledTime(ta.getID()) + estimatedRuntime : Long.MAX_VALUE;
            newSuspendableTaskAttempts.put(estimatedEndtime, ta);
          }
        }
      }
    }
    LOG.info("(bcho2) updating suspendable task attempts done, size "
        +newSuspendableTaskAttempts.size());
    this.suspendableTaskAttempts = newSuspendableTaskAttempts;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void handle(ReleaseEvent event) {
    switch (event.getType()) {
      case RELEASE_RESOURCES:
      {
        SortedMap<Long, TaskAttempt> taskAttempts = this.suspendableTaskAttempts; // Does making this reference actually help deal with concurrency?
        
        int memToRelease = event.getReleaseResource().getMemory();
        int tasksToRelease = memToRelease/memory + (memToRelease%memory == 0 ? 0 : 1);
        
        int fastForward = 0;
        if ("mid".equals(releaseStrategy)) {
          fastForward = (taskAttempts.size()-tasksToRelease)/2;
        } else if ("latest".equals(releaseStrategy)){
          fastForward = taskAttempts.size()-tasksToRelease;
        }
        LOG.info("(bcho2) release num tasks "+tasksToRelease
            +" size "+taskAttempts.size()
            +" fast forward "+fastForward);
        
        if ("none".equals(releaseStrategy)) { // Un-sort :(
          List<TaskAttempt> attempts = 
            new ArrayList<TaskAttempt>(taskAttempts.values());
          Collections.shuffle(attempts);
          for (TaskAttempt ta : attempts) {
            if (tasksToRelease > 0) {
              LOG.info("(bcho2) release ta "+ta.getID()
                  +" tasks to release "+tasksToRelease);
              eventHandler.handle(
                  new TaskAttemptEvent(ta.getID(),
                      TaskAttemptEventType.TA_SUSPEND));
              tasksToRelease--;
            }
          }
        } else {
          for (Entry<Long, TaskAttempt> entry : taskAttempts.entrySet()) {
            TaskAttempt ta = entry.getValue();
            if (ta.getState() != TaskAttemptState.RUNNING) {
              LOG.warn("(bcho2) suspendable task attempt not running!");
              continue;
            }
            if (fastForward > 0) {
              fastForward--;
              continue;
            }
            if (tasksToRelease > 0) {
              LOG.info("(bcho2) release ta "+ta.getID()
                  +" end time "+entry.getKey()
                  +" tasks to release "+tasksToRelease);
              eventHandler.handle(
                  new TaskAttemptEvent(ta.getID(),
                      TaskAttemptEventType.TA_SUSPEND));
              tasksToRelease--;
            } else {
              break;
            }
          }
        }
        
        break;
      }
      case RELEASED:
      {
        LOG.info("(bcho2) got released event, sending resume event");
        eventHandler.handle(
            new TaskEvent(event.getTaskId(),
                TaskEventType.T_RESUME));
        break;
      }
    }
  }
}
