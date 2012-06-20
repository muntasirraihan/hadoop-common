package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;

public class Suspender {
  private static final Log LOG = LogFactory.getLog(Suspender.class);
  
  private boolean doSuspend = false;
  private int stallTime = 0;
  private final org.apache.hadoop.mapred.TaskAttemptID taskId;
  private final TaskUmbilicalProtocol umbilical;
  
  private boolean doneSuspend = false;
  
  public Suspender(final org.apache.hadoop.mapred.TaskAttemptID taskId,
      final TaskUmbilicalProtocol umbilical,
      int stallTime) {
    this.taskId = taskId;
    this.umbilical = umbilical;
    this.stallTime = stallTime;
  }
  
  public void suspend(ReduceContext reducerContext,
      RecordWriter trackedRW, long keyCount) {
    LOG.info("(bcho2) RESUMEKEY "+keyCount);
    if (trackedRW != null && reducerContext != null) {
      LOG.info("(bcho2) closing, trackedRW "+trackedRW+" reducerContext "+reducerContext);
      try {
        trackedRW.close(reducerContext);
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
  
  public void stall() {
    LOG.info("(bcho2) stalling");
    for (int i = 0; i < 10; i++) {
      try {
        Thread.sleep(stallTime/10);
      } catch (InterruptedException e) {
        LOG.info("(bcho2) stalling interrupted, i: "+i);
      }
    }
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
}
