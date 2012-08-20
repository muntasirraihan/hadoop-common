package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;

public class PartialCommitter {
  private static final Log LOG = LogFactory.getLog(PartialCommitter.class);
  
  private final ReduceTask reduceTask;
  private final OutputCommitter committer;
  private final TaskUmbilicalProtocol umbilical;
  private final org.apache.hadoop.mapred.TaskAttemptID taskId;
  private final List<String> partialAttempts =
    new ArrayList<String>();
  private long firstKey = 0; // TODO: pass in the first key
  private long lastKey;
  private int partialCommitId = -1;
  // private String partialCommitId = "partialCommit"; // TODO: create a commit ID
  
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
  
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(2);
  }
  
  public RecordWriter partialCommit(ReduceContext reducerContext,
      RecordWriter trackedRW, long keyCount)
      {
    String partialCommitStr = "pc-"+idFormat.format(partialCommitId);
    this.lastKey = keyCount - 1;
    RecordWriter newTrackedRW = null;
    if (trackedRW != null && reducerContext != null) {
      LOG.info("(bcho2) closing, trackedRW "+trackedRW+" reducerContext "+reducerContext);
      try {
        trackedRW.close(reducerContext);
        // (i) rename file part -> part-firstKey-lastKey
        committer.renamePartialData(reducerContext, firstKey, lastKey);
        // (ii) mv taskAttemptPath -> taskAttemptPath-commitId
        committer.partialCommitTask(reducerContext, partialCommitStr);
        // TODO: (iii) create soft links in JobPath (likely do this at JobImpl?)

        // TODO: add way to find this stuff later, for commit
        partialAttempts.add(partialCommitStr);

      } catch (InterruptedException e) {
        LOG.warn("(bcho2) could not close", e);
      } catch (IOException e) {
        LOG.warn("(bcho2) could not close", e);
      }        
      try {
        newTrackedRW = reduceTask.initNewTrackingRecordWriter();
      } catch (InterruptedException e) {
        LOG.warn("(bcho2) could not init", e);
      } catch (IOException e) {
        LOG.warn("(bcho2) could not init", e);
      }
    } else {
      LOG.info("(bcho2) could not close, trackedRW "+trackedRW+" reducerContext "+reducerContext);
    }
    firstKey = lastKey + 1; // for next partial commit
    try {
      umbilical.donePartialCommit(taskId);
    } catch (IOException e) {
      LOG.info("(bcho2) Partial commit failed: ", e);
    }
    donePartialCommit = true;
    doPartialCommit = false;
    partialCommitId = -1;
    return newTrackedRW;
  }

  public void setDoPartialCommit(int partialCommitId) {
    LOG.info("(bcho2) setDoPartialCommit");
    this.doPartialCommit = true;
    this.partialCommitId = partialCommitId;
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
  
  public long getFirstKey() {
    return firstKey;
  }
  
  public long getLastKey() {
    return lastKey;
  }

  public void setLastKeyCount(long keyCount) {
    this.lastKey = keyCount-1;
  }

  public List<String> getPartialAttempts() {
    return partialAttempts;
  }
}
