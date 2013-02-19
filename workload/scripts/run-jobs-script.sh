#!/usr/bin/env bash

#defaults
ARGS="jar ../WorkGenYarn.jar org.apache.hadoop.examples.WorkGenYarn -conf ../workGenKeyValue_conf.xsl -libjars ../WorkGenYarn.jar"
BINDIR="../../bin"
OUTDIR="workGenLogs"
QUEUE="production"
DEADLINE="0"
INTERVAL="20"
DURATION="1"
NUMMAPS="2"
NUMREDUCES="4"
MAPRATIO="1.0"
REDRATIO="1.0"
SUSPEND_STRATEGY="none"
MAPRATIO_DISTRIBUTION="singleton"
REDRATIO_DISTRIBUTION="singleton"
HDFSIN="workGenInput"
HDFSIN_INDEX="0"
HDFSIN_NUM="64"
VERBOSE="false"

arg_type="none"

while (( "$#" )); do
  case $1 in
    --args)
      arg_type="args"
      ARGS=""
      ;;
    --bindir)
      arg_type="bindir"
      ;;
    --outdir)
      arg_type="outdir"
      ;;
    --queue)
      arg_type="queue"
      ;;
    --deadline)
      arg_type="deadline"
      ;;
    --interval)
      arg_type="interval"
      ;;
    --duration)
      arg_type="duration"
      ;;
    --nummaps)
      arg_type="nummaps"
      ;;
    --numreduces)
      arg_type="numreduces"
      ;;
    --mapratio)
      arg_type="mapratio"
      ;;
    --redratio)
      arg_type="redratio"
      ;;
    --mapratio-distribution)
      arg_type="mapratio-distribution"
      ;;
    --redratio-distribution)
      arg_type="redratio-distribution"
      ;;
    --hdfs-input)
      arg_type="hdfs-input"
      ;;
    --hdfs-input-num)
      arg_type="hdfs-input-num"
      ;;
    --verbose)
      arg_type="verbose"
      ;;
    --suspend-strategy)
      arg_type="suspend-strategy"
      ;;
    --sleep)
      arg_type="sleep"
      ;;
    --jobs)
      arg_type="jobs"
      ;;
    --clean)
      arg_type="clean"
      ;;
    *)
      if [ "$arg_type" = "args" ] ; then
        ARGS="$ARGS $1"
      elif [ "$arg_type" = "bindir" ] ; then
        BINDIR="$1"
      elif [ "$arg_type" = "outdir" ] ; then
        OUTDIR="$1"
      elif [ "$arg_type" = "queue" ] ; then
        QUEUE="$1"
      elif [ "$arg_type" = "deadline" ] ; then
	DEADLINE="$1"  
      elif [ "$arg_type" = "interval" ] ; then
	INTERVAL="$1"
      elif [ "$arg_type" = "duration" ] ; then
	DURATION="$1"
      elif [ "$arg_type" = "nummaps" ] ; then
	NUMMAPS="$1"
      elif [ "$arg_type" = "numreduces" ] ; then
	NUMREDUCES="$1"
      elif [ "$arg_type" = "mapratio" ] ; then
	MAPRATIO="$1"
      elif [ "$arg_type" = "redratio" ] ; then
	REDRATIO="$1"
      elif [ "$arg_type" = "mapratio-distribution" ] ; then
	MAPRATIO_DISTRIBUTION="$1"
      elif [ "$arg_type" = "redratio-distribution" ] ; then
	REDRATIO_DISTRIBUTION="$1"
      elif [ "$arg_type" = "hdfs-input" ] ; then
	HDFSIN="$1"
      elif [ "$arg_type" = "hdfs-input-num" ] ; then
	HDFSIN_NUM="$1"
      elif [ "$arg_type" = "verbose" ] ; then
	VERBOSE="$1"
      elif [ "$arg_type" = "suspend-strategy" ] ; then
	SUSPEND_STRATEGY="$1"
      elif [ "$arg_type" = "sleep" ] ; then
	echo "Sleeping $1"
        sleep $1
      elif [ "$arg_type" = "jobs" ] ; then
	echo "Running job $1 $SUSPEND_STRATEGY"
	$BINDIR/hadoop $ARGS \
	    -Dmapreduce.job.queuename=$QUEUE -Dmapreduce.job.deadline=$DEADLINE -Dmapreduce.job.suspend.strategy=$SUSPEND_STRATEGY \
	    -DworkGen.record-interval=$INTERVAL -DworkGen.sleep-duration=$DURATION \
	    -DworkGen.ratios.outputShuffleDistribution=$REDRATIO_DISTRIBUTION \
	    -DworkGen.ratios.shuffleInputDistribution=$REDRATIO_DISTRIBUTION \
	    -DworkGen.num-inputs=$NUMMAPS \
	    -m $NUMMAPS -r $NUMREDUCES -v $VERBOSE \
	    $HDFSIN $HDFSIN_INDEX $HDFSIN_NUM out-$1 $MAPRATIO $REDRATIO >> $OUTDIR/job-$1.txt 2>> $OUTDIR/job-$1.txt &
        # ./run-job-scripted.sh \
	#     --jobname $1 --bindir $BINDIR --outdir $OUTDIR $ARGS \
	#     -Dmapreduce.job.queuename=$QUEUE -Dmapreduce.job.suspend.strategy=$SUSPEND_STRATEGY \
	#     -DworkGen.record-interval=$INTERVAL -DworkGen.sleep-duration=$DURATION \
	#     -DworkGen.ratios.outputShuffleDistribution=$REDRATIO_DISTRIBUTION \
	#     -DworkGen.ratios.shuffleInputDistribution=$REDRATIO_DISTRIBUTION \
	#     -DworkGen.num-inputs=$NUMMAPS \
	#     -m $NUMMAPS -r $NUMREDUCES \
	#     $HDFSIN $HDFSIN_INDEX $HDFSIN_NUM out-$1 $MAPRATIO $REDRATIO &
	HDFSIN_INDEX=$(( $HDFSIN_INDEX + $NUMMAPS ))
	HDFSIN_INDEX=$(( $HDFSIN_INDEX % $HDFSIN_NUM ))
      elif [ "$arg_type" = "clean" ] ; then
	echo "Cleaning $1"
	$BINDIR/hdfs dfs -rm -r out-$1
      else
        echo "Unrecognized $arg_type"
      fi
      # arg_type="none"
      ;;
  esac
  shift
done
