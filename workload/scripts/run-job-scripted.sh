#!/usr/bin/env bash

# defaults
BINDIR="../../bin"
OUTDIR="workGenLogs"
JOBNAME="default"

more_conf=true

while $more_conf ; do

  if [ "$1" = "--bindir" ] ; then
    shift
    BINDIR=$1
    shift
  elif [ "$1" = "--outdir" ] ; then
    shift
    OUTDIR=$1
    shift
  elif [ "$1" = "--jobname" ] ; then
    shift
    JOBNAME=$1
    shift
  else
    more_conf=false # The rest are args
  fi
done

date
# Running rm before run, so we can inspect the results after.
echo "Starting Job job-$JOBNAME"
$BINDIR/hadoop $* >> $OUTDIR/job-$JOBNAME.txt 2>> $OUTDIR/job-$JOBNAME.txt

date
echo "Finished Job job-$JOBNAME"

echo "Removing output"
$BINDIR/hdfs dfs -rm -r out-$JOBNAME
