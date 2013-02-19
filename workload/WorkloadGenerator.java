package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class WorkloadGenerator {

  private static String OPT_QUEUE = "queue";
  private static String OPT_SLEEP_NORMALIZE = "sleep_normalize";
  private static String OPT_INPUT = "input";
  private static String OPT_RUN = "run";
  private static String OPT_HDFS_INPUTS = "num_hdfs_inputs";
  private static String OPT_MIN_MAPS = "min_maps";
  private static String OPT_MIN_REDUCES = "min_reduces";
  
  private static int JOB_ID = 0;
  private static int QUEUE_NAME = 1;
  private static int SUBMIT_TIMESTAMP = 2;
  private static int SLOTS_MILLIS_MAPS = 3;
  private static int SLOTS_MILLIS_REDUCES = 4;
  private static int TOTAL_MAPS = 5;
  private static int TOTAL_REDUCES = 6;
  private static int SPLIT_LENGTH = 7;
  
  private static int MILLIS_PER_RATIO = 700000;
  
  @SuppressWarnings("static-access")
  private static Options buildOptions() {
    Options opts = new Options();
    opts
    .addOption(
        OptionBuilder
        .withArgName("queue_name multiplier map_normalize reduce_normalize")
        .hasArgs(4)
        .create(OPT_QUEUE)
    )
    .addOption(
        OptionBuilder
        .withArgName("divide-millis")
        .hasArg()
        .isRequired()
        .create(OPT_SLEEP_NORMALIZE)
    )
    .addOption(
        OptionBuilder
        .withArgName("file")
        .hasArg()
        .isRequired()
        .create(OPT_INPUT)
    )
    .addOption(
        OptionBuilder
        .withArgName("type")
        .hasArg()
        .isRequired()
        .create(OPT_RUN)
    )
    .addOption(
        OptionBuilder
        .withArgName("int")
        .hasArg()
        .isRequired()
        .create(OPT_HDFS_INPUTS)
    )
    .addOption(
        OptionBuilder
        .withArgName("tasks")
        .hasArg()
        .create(OPT_MIN_MAPS)
    )
    .addOption(
        OptionBuilder
        .withArgName("tasks")
        .hasArg()
        .create(OPT_MIN_REDUCES)
    )
    ;
    return opts;
  }
  
  private static class QueueScaling {
    private String queueName;
    private Double multiplier;
    private int mapNormalize;
    private int reduceNormalize;
    
    public QueueScaling(String queueName, Double multiplier, int mapNormalize,
        int reduceNormalize) {
      this.queueName = queueName;
      this.multiplier = multiplier;
      this.mapNormalize = mapNormalize;
      this.reduceNormalize = reduceNormalize;
    }
  }
  
  private static class JobEvent implements Comparable<JobEvent> {
    private String jobName;
    private long timestamp;
    private int numMaps;
    private int numReduces;
    
    public JobEvent(String jobName, long timestamp, int numMaps, int numReduces) {
      this.jobName = jobName;
      this.timestamp = timestamp;
      this.numMaps = numMaps;
      this.numReduces = numReduces;
    }

    @Override
    public int compareTo(JobEvent that) {
      // TODO Auto-generated method stub
      return (int) (this.timestamp - that.timestamp);
    }
  }
  
  private static int runAnalyze(String path, Map<String, QueueScaling> queueMap, long sleepNormalize, int minMaps, int minReduces) throws IOException {
    BufferedReader input = new BufferedReader(new FileReader(path));
    
    String line;
    String[] splits;
    
    int job = 0;
    
    PriorityQueue<JobEvent> jobEvents = 
      new PriorityQueue<JobEvent>();
    
    while ((line = input.readLine()) != null) {
      splits = line.split("\t"); // .tsv
      if (splits.length < SPLIT_LENGTH) break;
      
      String jobName = splits[JOB_ID];
      String queue = splits[QUEUE_NAME];
      QueueScaling queueScaling = queueMap.get(queue);
      
      long timestamp = Long.parseLong(splits[SUBMIT_TIMESTAMP]);
      
      double slotsMillisMaps = Double.parseDouble(splits[SLOTS_MILLIS_MAPS]);
      double mapRatio = slotsMillisMaps / queueScaling.mapNormalize;
      
      double slotsMillisReduces = Double.parseDouble(splits[SLOTS_MILLIS_REDUCES]);
      double reduceRatio = slotsMillisReduces / queueScaling.reduceNormalize;
      
      
      int numMapsOrig = Integer.parseInt(splits[TOTAL_MAPS]);
      int numMaps = (int)Math.ceil(numMapsOrig * queueScaling.multiplier);
      
      int numReducesOrig = Integer.parseInt(splits[TOTAL_REDUCES]);
      int numReduces = (int)Math.ceil(numReducesOrig * queueScaling.multiplier);

      // Special cases
      if (numMaps <= minMaps && numReduces <= minReduces) {
        continue; // No need to run this job
      }  else if (numMaps <= minMaps) {
        // "Simulate" 0 maps
        numMaps = 1;
        mapRatio = 0.0;
      } else if (numReduces <= minReduces) {
        // "Simulate" 0 reduces
        numReduces = 1;
        reduceRatio = 0.0;
      }
      
      long OVERHEAD = 20000;
      
      long mapEndDuration = 0;
      JobEvent mapStart = new JobEvent(jobName, timestamp, numMaps, 0);
      jobEvents.add(mapStart);
      if (mapRatio > 0.0) {
        mapEndDuration = OVERHEAD + (long)Math.ceil(mapRatio*MILLIS_PER_RATIO/numMaps);
      } else {
        mapEndDuration = OVERHEAD;
      }
      JobEvent mapEnd = new JobEvent(jobName, timestamp+mapEndDuration, -numMaps, 0);
      jobEvents.add(mapEnd);
      
      long reduceEndDuration = 0;
      if (reduceRatio > 0.0) {
        JobEvent reduceStart = new JobEvent(jobName, timestamp+mapEndDuration, 0, numReduces); 
        jobEvents.add(reduceStart);
        
        reduceEndDuration = (long)Math.ceil(reduceRatio*MILLIS_PER_RATIO/numReduces); 
        JobEvent reduceEnd = new JobEvent(jobName, timestamp+mapEndDuration+reduceEndDuration, 0, -numReduces);
        jobEvents.add(reduceEnd);
      }


      System.out.println(jobName+"\t"+job+"\t"+numMaps+"\t"+numReduces+"\t"+mapEndDuration+"\t"+reduceEndDuration+"\t"+(mapEndDuration+reduceEndDuration));
      job++;
    }

    int runningMaps = 0;
    int runningReduces = 0;
    
    while(!jobEvents.isEmpty()) {
      JobEvent je = jobEvents.remove();
      
      runningMaps += je.numMaps;
      runningReduces += je.numReduces;
      
      System.out.println(je.timestamp+"\t"+runningMaps+"\t"+runningReduces+"\t"+je.jobName);
    }
    
    return job;
  }
  
  private static int runGenerate(String path, Map<String, QueueScaling> queueMap, long sleepNormalize, int numHdfsInputs, int minMaps, int minReduces) throws IOException {
    
    System.out.println("#!/usr/bin/env bash");
    System.out.println();
    System.out.println("CURRDIR=\"`pwd`\"");
    System.out.println("SCRIPTDIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"");
    System.out.println("#defaults");
    System.out.println("ARGS=\"jar ../WorkGenYarn.jar org.apache.hadoop.examples.WorkGenYarn -conf ../workGenKeyValue_conf.xsl -libjars ../WorkGenYarn.jar\"");
    System.out.println("BINDIR=\"../../bin\"");
    System.out.println("OUTDIR=\"workGenLogs\"");
    System.out.println("");
    System.out.println("#parse args");
    System.out.println("arg_type=\"none\"");
    System.out.println("");
    System.out.println("while (( \"$#\" )); do");
    System.out.println("  case $1 in");
    System.out.println("    --args)");
    System.out.println("      arg_type=\"args\"");
    System.out.println("      ARGS=\"\"");
    System.out.println("      ;;");
    System.out.println("    --bindir)");
    System.out.println("      arg_type=\"bindir\"");
    System.out.println("      ;;");
    System.out.println("    --outdir)");
    System.out.println("      arg_type=\"outdir\"");
    System.out.println("      ;;");
    System.out.println("    *)");
    System.out.println("      if [ \"$arg_type\" = \"args\" ] ; then");
    System.out.println("        ARGS=\"$ARGS $1\"");
    System.out.println("      elif [ \"$arg_type\" = \"bindir\" ] ; then");
    System.out.println("        BINDIR=\"$1\"");
    System.out.println("      elif [ \"$arg_type\" = \"outdir\" ] ; then");
    System.out.println("        OUTDIR=\"$1\"");
    System.out.println("      else");
    System.out.println("        echo \"Unrecognized $arg_type\"");
    System.out.println("      fi");
    System.out.println("      ;;");
    System.out.println("  esac");
    System.out.println("  shift");
    System.out.println("done");
    System.out.println("");
    System.out.println("#run");
    System.out.println("if [ -d \"$OUTDIR\" ]; then rm -r \"$OUTDIR\"; fi");
    System.out.println("mkdir \"$OUTDIR\"");
    System.out.println();
    System.out.println("cd $SCRIPTDIR");
    System.out.println("./run-jobs-script.sh \\");
    System.out.println(" --bindir $BINDIR --outdir $OUTDIR \\");
    System.out.println(" --args $ARGS \\");
    System.out.println(" --redratio-distribution uniform --suspend-strategy shortest \\");
    System.out.println(" --hdfs-input workGenInput \\");
    System.out.println(" --interval 9 --duration 1 \\");
    // System.out.println(" --interval 3 --duration 1 \\");
    System.out.println(" --hdfs-input-num "+numHdfsInputs+" \\");
    
    BufferedReader input = new BufferedReader(new FileReader(path));
    
    String line;
    String[] splits;
    
    long prevTimestamp = -1;
    int job = 0;
    int totalMaps = 0;
    
    while ((line = input.readLine()) != null) {
      splits = line.split("\t"); // .tsv
      if (splits.length < SPLIT_LENGTH) break;
      
      String jobName = splits[JOB_ID];
      String queue = splits[QUEUE_NAME];
      QueueScaling queueScaling = queueMap.get(queue);
      
      long timestamp = Long.parseLong(splits[SUBMIT_TIMESTAMP]);
      long sleep = prevTimestamp < 0 ? 0 : (timestamp - prevTimestamp);
      sleep = sleep / sleepNormalize;
      prevTimestamp = timestamp;
      
      double slotsMillisMaps = Double.parseDouble(splits[SLOTS_MILLIS_MAPS]);
      double mapRatio = slotsMillisMaps / queueScaling.mapNormalize;
      
      double slotsMillisReduces = Double.parseDouble(splits[SLOTS_MILLIS_REDUCES]);
      double reduceRatio = slotsMillisReduces / queueScaling.reduceNormalize;

      int numMapsOrig = Integer.parseInt(splits[TOTAL_MAPS]);
      int numMaps = (int)Math.ceil(numMapsOrig * queueScaling.multiplier);

      int numReducesOrig = Integer.parseInt(splits[TOTAL_REDUCES]);
      int numReduces = (int)Math.ceil(numReducesOrig * queueScaling.multiplier);
      
      mapRatio = mapRatio / numMaps;
      reduceRatio = reduceRatio / numReduces;

      // Special cases
      if (numMaps <= minMaps && numReduces <= minReduces) {
        System.out.printf(" --sleep %d \\\n", sleep);
        continue; // No need to run this job
      } else if (numMaps <= minMaps) {
        // "Simulate" 0 maps
        numMaps = 1;
        mapRatio = 0.0;
      } else if (numReduces <= minReduces) {
        // "Simulate" 0 reduces
        numReduces = 1;
        reduceRatio = 0.0;
      }
      
      System.out.printf(
          " --sleep %d \\\n --queue %s --mapratio %.2f --redratio %.2f --nummaps %d --numreduces %d --jobs %d\\\n",
          sleep, queue, mapRatio, reduceRatio, numMaps, numReduces, job, jobName
          );
      
      job++;
      totalMaps += numMaps;
    }
    System.out.println();
    System.out.println("until [ `$BINDIR/hadoop job -list | grep \"Total jobs\" | awk -F':' '{print $2}'` -eq 0 ]; do");
    System.out.println("  echo \"Waiting for job to complete, sleeping 5 mins.\";");
    System.out.println("  sleep 300;");
    System.out.println("done");
    System.out.println();
    System.out.println("cd $CURRDIR");
    return job;
  }

  private static Map<String, QueueScaling> parseQueues(String[] queueOpts) {
    Map<String, QueueScaling> queueMap = new HashMap<String, QueueScaling>();
    String queueName = "";
    double multiplier = 0.0;
    int mapNormalize = 0;
    int reduceNormalize = 0;
    
    for (int i = 0; i < queueOpts.length; i++) {
      if (i % 4 == 0) {
        queueName = queueOpts[i];
      } else if (i % 4 == 1) {
        multiplier = Double.parseDouble(queueOpts[i]);
      } else if (i % 4 == 2) {
        mapNormalize = Integer.parseInt(queueOpts[i]);
      } else if (i % 4 == 3) {
        reduceNormalize = Integer.parseInt(queueOpts[i]);
        
        QueueScaling queueScaling = new QueueScaling(queueName, multiplier, mapNormalize, reduceNormalize);
        queueMap.put(queueName, queueScaling);
      }
    }
    return queueMap;
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    CommandLine commandLine;
    Options opts = buildOptions();
    CommandLineParser parser = new GnuParser();
    
    try {
      commandLine = parser.parse(opts, args);
    } catch (ParseException e) {
      System.err.println("options parsing failed: "+e.getMessage());

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("", opts);
      return;
    }
    
    long sleepNormalize = Long.parseLong(commandLine.getOptionValue(OPT_SLEEP_NORMALIZE));
    String inputPath = commandLine.getOptionValue(OPT_INPUT);
        
    Map<String, QueueScaling> queueMap = parseQueues(commandLine.getOptionValues(OPT_QUEUE));
    
    int minMaps = 0;
    int minReduces = 0;
    if (commandLine.hasOption(OPT_MIN_MAPS)) {
      minMaps = Integer.parseInt(commandLine.getOptionValue(OPT_MIN_MAPS));
    }
    if (commandLine.hasOption(OPT_MIN_REDUCES)) {
      minReduces = Integer.parseInt(commandLine.getOptionValue(OPT_MIN_REDUCES));
    }
    
    String run = commandLine.getOptionValue(OPT_RUN);
    if ("analyze".equals(run)) {
      runAnalyze(inputPath, queueMap, sleepNormalize, minMaps, minReduces);
    } else if ("generate".equals(run)) {
      int numHdfsInputs = Integer.parseInt(commandLine.getOptionValue(OPT_HDFS_INPUTS));
      runGenerate(inputPath, queueMap, sleepNormalize, numHdfsInputs, minMaps, minReduces);
    } else {
      System.err.println("Unrecognized run option "+run);
    }
  }
}
