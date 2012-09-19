package org.apache.hadoop.mapreduce.v2.app.release;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
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
  private final EventHandler eventHandler;
  private final TaskRuntimeEstimator estimator;
  private final String releaseStrategy;
  private final int releaserPollInterval;
  
  private List<EstimatedAttempt> suspendableTaskAttempts;
  private Set<TaskAttempt> suspendedTaskAttempts = new HashSet<TaskAttempt>();
  
  public Releaser(Configuration conf, AppContext context, TaskRuntimeEstimator estimator) {
    super(Releaser.class.getName());
    this.conf = conf;
    this.context = context;
    
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

  static class EstimatedAttempt implements Comparable<EstimatedAttempt> {
    private final long estimate;
    private final TaskAttempt attempt;

    public EstimatedAttempt(long estimate, TaskAttempt attempt) {
      this.estimate = estimate;
      this.attempt = attempt;
    }

    public long getEstimate() {
      return estimate;
    }

    public TaskAttempt getAttempt() {
      return attempt;
    }
    
    @Override
    public int compareTo(EstimatedAttempt that) {
      return Long.valueOf(this.estimate).compareTo(that.estimate);
    }
  }
  
  private void updateSuspendableTaskAttempts() {
    LOG.info("(bcho2) updating suspendable task attempts start");
    List<EstimatedAttempt> newSuspendableTaskAttempts =
      new ArrayList<EstimatedAttempt>();
    for (Job job : context.getAllJobs().values()) {
      for (Task task : job.getTasks(TaskType.REDUCE).values()) {
        for (TaskAttempt ta : task.getAttempts().values()) {
          if (ta.getState() == TaskAttemptState.RUNNING) {
            long estimatedRuntime = estimator.estimatedRuntime(ta.getID());
            long estimatedEndtime = (estimatedRuntime > 0) ? 
                estimator.attemptEnrolledTime(ta.getID()) + estimatedRuntime : Long.MAX_VALUE;
            newSuspendableTaskAttempts.add(new EstimatedAttempt(estimatedEndtime, ta));
          }
        }
      }
    }
    LOG.info("(bcho2) updating suspendable task attempts done, size "
        +newSuspendableTaskAttempts.size());
    this.suspendableTaskAttempts = newSuspendableTaskAttempts;
  }
  
  private int tasMemory(List<TaskAttempt> taskAttempts) {
    int total = 0;
    for (TaskAttempt ta : taskAttempts) {
      total += ta.getResourceCapability().getMemory();
    }
    return total;
  }

  private int suspendTasksRandom(ResourceRequest request) {
    List<EstimatedAttempt> estimatedAttempts =
      new ArrayList<EstimatedAttempt>(this.suspendableTaskAttempts);
    Collections.shuffle(estimatedAttempts);
    return suspendTasks(request, estimatedAttempts);
  }

  private int suspendTasksSRT(ResourceRequest request) {
    List<EstimatedAttempt> estimatedAttempts =
      new ArrayList<EstimatedAttempt>(this.suspendableTaskAttempts);
    Collections.sort(estimatedAttempts);
    return suspendTasks(request, estimatedAttempts);
  }

  private int suspendTasksLRT(ResourceRequest request) {
    List<EstimatedAttempt> estimatedAttempts =
      new ArrayList<EstimatedAttempt>(this.suspendableTaskAttempts);
    Collections.sort(estimatedAttempts, Collections.reverseOrder());
    return suspendTasks(request, estimatedAttempts);
  }

  @SuppressWarnings("unchecked")
  private int suspendTasks(ResourceRequest request, List<EstimatedAttempt> estimatedAttempts) {
    Map<String, List<TaskAttempt>> hostTAs = new HashMap<String, List<TaskAttempt>>();
    int memory = request.getCapability().getMemory();
    int numContainers = request.getNumContainers();
    
    LOG.info("(bcho2) memory "+memory+
        " container "+numContainers+
        " taskAttempts.size "+estimatedAttempts.size());
    
    for (EstimatedAttempt eta : estimatedAttempts) {
      if (numContainers <= 0) break;     
      TaskAttempt ta = eta.getAttempt();
      if (suspendedTaskAttempts.contains(ta)) continue;
      
      if (ta.getState() != TaskAttemptState.RUNNING) {
        LOG.warn("(bcho2) suspendable task attempt not running!");
        continue;
      }
      
      String taHost = ta.getNodeHostName();
      int taMemory = ta.getResourceCapability().getMemory();
      if (!hostTAs.containsKey(taHost) && taMemory >= memory) { // bypass map completely
        LOG.info("(bcho2) release ta "+ta.getID()
            +" host "+taHost
            +" memory "+taMemory
            +" containers remaining "+numContainers);
        eventHandler.handle(
            new TaskAttemptEvent(ta.getID(),
                TaskAttemptEventType.TA_SUSPEND));
        suspendedTaskAttempts.add(ta);
        numContainers--;
      } else {
        List<TaskAttempt> taList = hostTAs.get(taHost);
        if (taList == null) {
          taList = new ArrayList<TaskAttempt>();
          hostTAs.put(taHost, taList);
        }
        taList.add(ta);
        
        if (tasMemory(taList) >= memory) {
          for (TaskAttempt suspendTa : taList) {
            LOG.info("(bcho2) release ta "+suspendTa.getID()
                +" host "+suspendTa.getNodeHostName()
                +" memory "+suspendTa.getResourceCapability().getMemory()
                +" containers remaining "+numContainers);
            eventHandler.handle(
                new TaskAttemptEvent(suspendTa.getID(),
                    TaskAttemptEventType.TA_SUSPEND));
            suspendedTaskAttempts.add(ta);
            numContainers--;            
          }
          hostTAs.remove(taHost); // or, taList.clear()?
        }
      }
    }
    
    return 0;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void handle(ReleaseEvent event) {
    switch (event.getType()) {
      case RELEASE_RESOURCES:
      {
        // TODO: When you have a request with large container sizes, releasing may be challenging.
        // TODO: Somehow, we need to make sure the multiple tasks that make up the container size are on the same node.
        // TODO: Implemented for SRT, need for other variants as well.
        if ("shortest".equals(releaseStrategy)) {
          suspendTasksSRT(event.getReleaseRequest());
        } else if ("longest".equals(releaseStrategy)) {
          suspendTasksLRT(event.getReleaseRequest());
        } else {
          suspendTasksRandom(event.getReleaseRequest());
        }

        
        // TODO: In fact, the way this is currently implemented, jobs with Map tasks running will have trouble too,
        // TODO: because when checking releasable memory on RM-side we don't take Map vs Reduce into account.
        // TODO: Pretty sad...
        
/*        
        int memToRelease = event.getReleaseRequest().getMemory();
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
*/      
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
