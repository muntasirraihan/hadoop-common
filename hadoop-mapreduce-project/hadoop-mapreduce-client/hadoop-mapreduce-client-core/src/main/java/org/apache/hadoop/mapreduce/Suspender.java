package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;

public class Suspender {
  private static final Log LOG = LogFactory.getLog(Suspender.class);
  
  private boolean doSuspend = false;
  private final org.apache.hadoop.mapred.TaskAttemptID taskId;
  private final List<String> suspendedTaskIds;
  private final OutputCommitter committer;
  private final TaskUmbilicalProtocol umbilical;
  
  private boolean doneSuspend = false;
  private long firstKey;
  private long lastKey;
  
  public Suspender(final OutputCommitter committer,
      final TaskUmbilicalProtocol umbilical,
      final org.apache.hadoop.mapred.TaskAttemptID taskId,
      long firstKey,
      final List<String> suspendedTaskIds) {
    this.committer = committer;
    this.umbilical = umbilical;
    this.taskId = taskId;
    this.firstKey = firstKey;
    this.suspendedTaskIds = suspendedTaskIds;
  }
  
  public void suspend(ReduceContext reducerContext,
      RecordWriter trackedRW, long keyCount) {
    for (String idStr : suspendedTaskIds) {
      LOG.info("(bcho2) SUSPENDED "+idStr);
    }
    LOG.info("(bcho2) SUSPENDED "+ taskId);
    LOG.info("(bcho2) RESUMEKEY "+keyCount);
    this.lastKey = keyCount - 1;
    if (trackedRW != null && reducerContext != null) {
      LOG.info("(bcho2) closing, trackedRW "+trackedRW+" reducerContext "+reducerContext);
      try {
        trackedRW.close(reducerContext);
        committer.renamePartialData(reducerContext, firstKey, lastKey);
      } catch (InterruptedException e) {
        LOG.warn("(bcho2) could not close", e);
      } catch (IOException e) {
        LOG.warn("(bcho2) could not close", e);
      }
    } else {
      LOG.info("(bcho2) could not close, trackedRW "+trackedRW+" reducerContext "+reducerContext);
    }
    try {
      umbilical.doneSuspend(taskId);
    } catch (IOException e) {
      LOG.info("(bcho2) Suspend failed: ", e);
    }
    // TODO: for StatefulSuspendableReducer, stall cannot come here. Must reconcile this whole stall business!!! (bcho2)
    // stall();
    doneSuspend = true;
  }

  public void setDoSuspend(boolean doSuspend) {
    LOG.info("(bcho2) setDoSuspend");
    this.doSuspend = doSuspend;
  }

  public boolean isDoSuspend() {
    return doSuspend;
  }
  
  public boolean isDoneSuspend() {
    return doneSuspend;
  }
  
  public void log(String info) {
    LOG.info("(bcho2) "+info);
  }
  
  public long getFirstKey() {
    return firstKey;
  }
  
  public long getLastKey() {
    return lastKey;
  }

  public void setLastKeyCount(long keyCount) {
    this.lastKey = keyCount-1;
  }
}
