/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.Merger.MergeQueue;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.StatefulSuspendableReducer;
import org.apache.hadoop.mapreduce.Suspender;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;

/** A Reduce task. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;


  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
  private Counters.Counter fileOutputByteCounter =
    getCounters().findCounter(FileOutputFormatCounter.BYTES_WRITTEN);

  boolean doSuspend = false;
  RawKeyValueIterator rIter = null;
  org.apache.hadoop.mapreduce.RecordWriter trackedRW = null;
  org.apache.hadoop.mapreduce.Reducer.Context reducerContext = null;
  private final List<String> resumePaths = new ArrayList<String>();
  Path resumeLogPath = null;
  long resumeKeyNumber = 0;
  boolean resumeIsStateful = false;
  
  long stallKeyNumber = 0;
  int stallTime = 0;
  
  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
    new Comparator<FileStatus>() {
      public int compare(FileStatus a, FileStatus b) {
        if (a.getLen() < b.getLen())
          return -1;
        else if (a.getLen() == b.getLen())
          if (a.getPath().toString().equals(b.getPath().toString()))
            return 0;
          else
            return -1; 
        else
          return 1;
      }
  };
  
  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
    new TreeSet<FileStatus>(mapOutputFileComparator);

  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  
  public void setSuspendedContainerLogDirStr(String suspendedContainerLogDirStr) {
    this.suspendedContainerLogDirStr = suspendedContainerLogDirStr;
  }

  public String getSuspendedContainerLogDirStr() {
    return suspendedContainerLogDirStr;
  }

  public void setSuspendedAttemptStr(String suspendedAttemptStr) {
    this.suspendedAttemptStr = suspendedAttemptStr;
  }

  public String getSuspendedAttemptStr() {
    return suspendedAttemptStr;
  }

  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer.
  private Path[] getMapFiles(FileSystem fs, boolean isLocal) 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    if (isLocal) {
      // for local jobs
      for(int i = 0; i < numMaps; ++i) {
        fileList.add(mapOutputFile.getInputFile(i));
      }
    } else { // (bcho2) -- this code doesn't ever run?! mapOutputFilesOnDisk is unnecessary?
      // for non local jobs
      for (FileStatus filestatus : mapOutputFilesOnDisk) {
        fileList.add(filestatus.getPath());
      }
    }
    return fileList.toArray(new Path[0]);
  }

  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // start thread that will handle communication with parent
    TaskReporter reporter = startReporter(umbilical);
    
    if (suspendedContainerLogDirStr != null) {
      LOG.info("(bcho2) suspended logdir: "+suspendedContainerLogDirStr);
      try {
        resumeParse();
      } catch (Exception e) {
        LOG.error("(bcho2) exception", e);
      }
    }
    
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }
    
    // Initialize the codec
    codec = initCodec();
    
    boolean isLocal = false; 
    // local if
    // 1) framework == local or
    // 2) framework == null and job tracker address == local
    String framework = job.get(MRConfig.FRAMEWORK_NAME);
    String masterAddr = job.get(MRConfig.MASTER_ADDRESS, "local");
    if ((framework == null && masterAddr.equals("local"))
        || (framework != null && framework.equals(MRConfig.LOCAL_FRAMEWORK_NAME))) {
      isLocal = true;
    }

    
    if (!isLocal && resumePaths.size() == 0) {
      Class combinerClass = conf.getCombinerClass();
      CombineOutputCollector combineCollector = 
        (null != combinerClass) ? 
 	     new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;

      Shuffle shuffle = 
        new Shuffle(getTaskID(), job, FileSystem.getLocal(job), umbilical, 
                    super.lDirAlloc, reporter, codec, 
                    combinerClass, combineCollector, 
                    spilledRecordsCounter, reduceCombineInputCounter,
                    shuffledMapsCounter,
                    reduceShuffleBytes, failedShuffleCounter,
                    mergedMapOutputsCounter,
                    taskStatus, copyPhase, sortPhase, this,
                    mapOutputFile);
      rIter = shuffle.run();
    } else if (!isLocal && resumePaths.size() > 0) {
      LOG.info("(bcho2) trying to use the resumePaths list");
      copyPhase.complete();
      Path[] paths = new Path[resumePaths.size()];
      int ind = 0;
      for (String s : resumePaths) {
        paths[ind] = new Path(s);
        ind++;
      }
      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      rIter = Merger.merge(job, rfs, job.getMapOutputKeyClass(),
                           job.getMapOutputValueClass(), codec, 
                           paths,
                           !conf.getKeepFailedTaskFiles(), 
                           job.getInt(JobContext.IO_SORT_FACTOR, 100),
                           new Path(getTaskID().toString()), 
                           job.getOutputKeyComparator(),
                           reporter, spilledRecordsCounter, null, null);
    } else {
      // local job runner doesn't have a copy phase
      copyPhase.complete();
      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      rIter = Merger.merge(job, rfs, job.getMapOutputKeyClass(),
                           job.getMapOutputValueClass(), codec, 
                           getMapFiles(rfs, true),
                           !conf.getKeepFailedTaskFiles(), 
                           job.getInt(JobContext.IO_SORT_FACTOR, 100),
                           new Path(getTaskID().toString()), 
                           job.getOutputKeyComparator(),
                           reporter, spilledRecordsCounter, null, null);
    }
    
    // (bcho2) TODO: stall, nasty hack for testing
    // - If attemptId = first one
    //     just stall here (waiting for suspend)
    stallKeyNumber = job.getLong("mapreduce.bcho2.stall.key", 0);
    stallTime = job.getInt("mapreduce.bcho2.stall.time", 0);
    LOG.info("(bcho2) stallTime "+stallTime+" stallKeyNumber "+stallKeyNumber);
    
    suspender = new Suspender(getTaskID(), umbilical, stallTime);
    
    // (bcho2) "reverse engineering"
    MergeQueue mergeQueue = (MergeQueue)rIter;
    if (mergeQueue.segments == null) {
      LOG.info("(bcho2)mergeQueue.segments null");
    } else {
      LOG.info("(bcho2)mergeQueue.segments-- size:" + mergeQueue.segments.size());
      for (Object segment : mergeQueue.segments) {
        LOG.info("(bcho2)mergeQueue.seqments--  " + segment);
      }
    }
    
    // free up the data structures
    mapOutputFilesOnDisk.clear();
    
    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE); 
    statusUpdate(umbilical);
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    if (useNewApi) {
      runNewReducer(job, umbilical, reporter, rIter, comparator, 
                    keyClass, valueClass);
    } else {
      runOldReducer(job, umbilical, reporter, rIter, comparator, 
                    keyClass, valueClass);
    }
    done(umbilical, reporter);
  }

  private void resumeParse() throws AccessControlException, FileNotFoundException, IOException {
    resumeLogPath = new Path(suspendedContainerLogDirStr, TaskLog.LogName.SYSLOG.toString());

    BufferedReader reader = new BufferedReader(new FileReader(resumeLogPath.toString()));
    
    int prevCounter = -1;
    String line = null;
    String[] split = null;
    while ((line = reader.readLine()) != null) {
      split = line.split("\\s+");
      if (split.length > 7 && "(bcho2)".equals(split[5]) && "SEGMENT".equals(split[6])) {
        int counter = Integer.parseInt(split[7]);
        if (counter != prevCounter) {
          resumePaths.clear();
          prevCounter = counter;
        }
        if (split.length > 9) {
          resumePaths.add(split[9]);
        }
      } else if (split.length > 7 && "(bcho2)".equals(split[5]) && "RESUMEKEY".equals(split[6])) {
        resumeKeyNumber = Long.parseLong(split[7]);
      } else if (split.length > 6 && "(bcho2)".equals(split[5]) && "STATEFUL".equals(split[6])) {
        resumeIsStateful = true;
      }
    }
    if (resumePaths.size() == 0) {
      LOG.error("(bcho2) no paths to resume!"); 
    } else {
      for (String p : resumePaths) {
        LOG.info("(bcho2) resume path: "+p); 
      }
    }
    reader.close();
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldReducer(JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException {
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    String finalName = getOutputName(getPartition());

    final RecordWriter<OUTKEY, OUTVALUE> out = new OldTrackingRecordWriter<OUTKEY, OUTVALUE>(
        this, job, reporter, finalName);

    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new OutputCollector<OUTKEY,OUTVALUE>() {
        public void collect(OUTKEY key, OUTVALUE value)
          throws IOException {
          out.write(key, value);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };
    
    // apply reduce function
    try {
      //increment processed counter only if skipping feature is enabled
      boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job)>0 &&
        SkipBadRecords.getAutoIncrReducerProcCount(job);
      
      ReduceValuesIterator<INKEY,INVALUE> values = isSkipping() ? 
          new SkippingReduceValuesIterator<INKEY,INVALUE>(rIter, 
              comparator, keyClass, valueClass, 
              job, reporter, umbilical) :
          new ReduceValuesIterator<INKEY,INVALUE>(rIter, 
          job.getOutputValueGroupingComparator(), keyClass, valueClass, 
          job, reporter);
      values.informReduceProgress();
      while (values.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(values.getKey(), values, collector, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
        }
        values.nextKey();
        values.informReduceProgress();
      }

      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
  }

  static class OldTrackingRecordWriter<K, V> implements RecordWriter<K, V> {

    private final RecordWriter<K, V> real;
    private final org.apache.hadoop.mapred.Counters.Counter reduceOutputCounter;
    private final org.apache.hadoop.mapred.Counters.Counter fileOutputByteCounter;
    private final Statistics fsStats;

    @SuppressWarnings({ "deprecation", "unchecked" })
    public OldTrackingRecordWriter(ReduceTask reduce, JobConf job,
        TaskReporter reporter, String finalName) throws IOException {
      this.reduceOutputCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;
      Statistics matchedStats = null;
      if (job.getOutputFormat() instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      }
      fsStats = matchedStats;

      FileSystem fs = FileSystem.get(job);
      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = job.getOutputFormat().getRecordWriter(fs, job, finalName,
          reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      reduceOutputCounter.increment(1);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }

  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileOutputByteCounter;
    private final Statistics fsStats;

    @SuppressWarnings("unchecked")
    NewTrackingRecordWriter(ReduceTask reduce,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws InterruptedException, IOException {
      this.outputRecordCounter = reduce.reduceOutputCounter;
      this.fileOutputByteCounter = reduce.fileOutputByteCounter;

      Statistics matchedStats = null;
      if (reduce.outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(taskContext), taskContext.getConfiguration());
      }

      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes(fsStats);
      this.real = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) reduce.outputFormat
          .getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.close(context);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      long bytesOutPrev = getOutputBytes(fsStats);
      real.write(key,value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      outputRecordCounter.increment(1);
    }

    private long getOutputBytes(Statistics stats) {
      return stats == null ? 0 : stats.getBytesWritten();
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass
                     ) throws IOException,InterruptedException, 
                              ClassNotFoundException {
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    this.rIter = new RawKeyValueIterator() {
     
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        reporter.setProgress(rawIter.getProgress().getProgress()); 
        return ret;
      }
    };
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job,
          getTaskID(), reporter);
    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    trackedRW = 
      new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(this, taskContext);
    job.setBoolean("mapred.skip.on", isSkipping());
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    reducerContext = createReduceContext(reducer, job, getTaskID(),
                                               this.rIter, reduceInputKeyCounter, 
                                               reduceInputValueCounter, 
                                               trackedRW,
                                               committer,
                                               reporter, comparator, keyClass,
                                               valueClass,
                                               suspender);

    if (reducer instanceof StatefulSuspendableReducer) { // TODO (bcho2) test this
      LOG.info("(bcho2) StatefulSuspendableReducer");
      StatefulSuspendableReducer<INKEY,INVALUE,OUTKEY,OUTVALUE> statefulReducer =
        (StatefulSuspendableReducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)reducer;
      statefulReducer.setSuspender(suspender);
      if (resumeIsStateful) {
        statefulReducer.setLogPath(resumeLogPath);
      }
    } else {
      LOG.info("(bcho2) NOT instanceof StatefulSuspendableReducer");
    }
    
    // (bcho2) Resume
    boolean isNextKey = true;
    for (int i = 0; i < resumeKeyNumber; i++) {
      isNextKey = reducerContext.nextKey();
    }
    LOG.info("(bcho2) isNextKey "+isNextKey);
    reducer.run(reducerContext);
    trackedRW.close(reducerContext);
  }
}
