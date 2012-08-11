package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class PartialCommitter<OUTKEY, OUTVALUE> {
  private static final Log LOG = LogFactory.getLog(PartialCommitter.class);
  
  private final ReduceTask reduceTask;
  private final OutputCommitter committer;
  private final TaskUmbilicalProtocol umbilical;
  private final org.apache.hadoop.mapred.TaskAttemptID taskId;
  
  private boolean doPartialCommit = false;
  private boolean donePartialCommit = false;
  
  public PartialCommitter(final ReduceTask reduceTask,
      final OutputCommitter committer,
      final TaskUmbilicalProtocol umbilical,
      final org.apache.hadoop.mapred.TaskAttemptID taskId) {
    this.reduceTask = reduceTask;
    this.committer = committer;
    this.umbilical = umbilical;
    this.taskId = taskId;
  }
  
  public RecordWriter partialCommit(ReduceContext reducerContext,
      RecordWriter trackedRW)
      {
    RecordWriter newTrackedRW = null;
    if (trackedRW != null && reducerContext != null) {
      LOG.info("(bcho2) closing, trackedRW "+trackedRW+" reducerContext "+reducerContext);
      try {
        trackedRW.close(reducerContext);

        // Move to _partialcommit location
        ((FileOutputCommitter)committer).renameFileWithSuffix(reducerContext, 1);
        
        newTrackedRW = reduceTask.initNewTrackingRecordWriter();
      } catch (InterruptedException e) {
        LOG.warn("(bcho2) could not close", e);
      } catch (IOException e) {
        LOG.warn("(bcho2) could not close", e);
      }
    } else {
      LOG.info("(bcho2) could not close, trackedRW "+trackedRW+" reducerContext "+reducerContext);
    }
    try {
      umbilical.donePartialCommit(taskId);
    } catch (IOException e) {
      LOG.info("(bcho2) Partial commit failed: ", e);
    }
    donePartialCommit = true;
    doPartialCommit = false;
    return newTrackedRW;
  }

  public void setDoPartialCommit(boolean doPartialCommit) {
    LOG.info("(bcho2) setDoPartialCommit");
    this.doPartialCommit = doPartialCommit;
  }

  public boolean isDoPartialCommit() {
    return doPartialCommit;
  }
  
  public boolean isDonePartialCommit() {
    return donePartialCommit;
  }
  
  public void log(String info) {
    LOG.info("(bcho2) "+info);
  }
}
