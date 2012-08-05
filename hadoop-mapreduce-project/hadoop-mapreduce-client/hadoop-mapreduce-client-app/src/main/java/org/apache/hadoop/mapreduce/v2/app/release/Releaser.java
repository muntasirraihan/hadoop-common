package org.apache.hadoop.mapreduce.v2.app.release;

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
import org.apache.hadoop.yarn.event.EventHandler;

public class Releaser implements EventHandler<ReleaseEvent> {
  public enum EventType {
    RELEASE_RESOURCES,
    RELEASED
  }

  private static final Log LOG = LogFactory.getLog(Releaser.class);
  
  private final Configuration conf;
  private final AppContext context;
  private final int memory;
  private final EventHandler eventHandler;
  
  public Releaser(Configuration conf, AppContext context) {
    this.conf = conf;
    this.context = context;
    
    this.memory = conf.getInt(MRJobConfig.REDUCE_MEMORY_MB,
                              MRJobConfig.DEFAULT_REDUCE_MEMORY_MB);
    this.eventHandler = context.getEventHandler();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void handle(ReleaseEvent event) {
    switch (event.getType()) {
      case RELEASE_RESOURCES:
      {
        int memToRelease = event.getReleaseResource().getMemory();
        LOG.info("(bcho2)"
            +" release memory "+memToRelease);
        for (Job job : context.getAllJobs().values()) {
          LOG.debug("(bcho2) mock:"
              +" job "+job.getID()
              +" with state "+job.getState()
              +" has reduce tasks: ");
          for (Task task : job.getTasks(TaskType.REDUCE).values()) {
            LOG.debug("(bcho2) mock:  "
                +" task "+task.getID()
                +" with state "+task.getState()
                +" has attempts: ");
            for (TaskAttempt ta : task.getAttempts().values()) {
              LOG.debug("(bcho2) mock:     "
                +" ta "+ta.getID()
                +" with state "+ta.getState()
                +" on container "+ta.getAssignedContainerID());
              if (memToRelease <= 0) {
                return;
              }
              if (ta.getState() == TaskAttemptState.RUNNING) {
                memToRelease -= memory;
                LOG.info("(bcho2) suspend ta "+ta.getID()
                    +" memory to release "+memToRelease);
                eventHandler.handle(
                    new TaskAttemptEvent(ta.getID(),
                        TaskAttemptEventType.TA_SUSPEND));
              }
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
