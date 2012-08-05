package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class StatefulSuspendableReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  protected Path logPath = null;
  protected Suspender suspender = null;

  protected void serializeSuspendState() {
    // NOTHING
  }

  protected void deserializeSuspendState() {
    // NOTHING
  }

  public void setLogPath(Path logPath) {
    this.logPath = logPath;
  }

  public void setSuspender(Suspender suspender) {
    this.suspender = suspender;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public void run(Context context) throws IOException, InterruptedException {
    suspender.log("StatefulSuspendableReducer run started");
    setup(context);
    if (logPath != null) {
      deserializeSuspendState();
    }
    while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
      // If a back up store is used, reset it
      ((ReduceContext.ValueIterator)
          (context.getValues().iterator())).resetBackupStore();
    }
    if (suspender != null && suspender.isDoneSuspend()) {
      suspender.log("in serialize");
      serializeSuspendState();
      // TODO: (bcho2) not sure if this works anymore, without stalling.      
      // suspender.stall();
      // System.exit(1);
    } else {
      suspender.log("did not get in serialize, suspender="+suspender+" isDoneSuspend()="+suspender.isDoneSuspend());
    }
    cleanup(context);
  }
}
