#!/bin/bash
clear

VERSION=0.23.3

HADOOP_HOME=$HOME/hadoop/hadoop-$VERSION-SNAPSHOT
HADOOP_SOURCE=$HOME/natjam/hadoop-common
JOB_OUTPUT_DIR=$HADOOP_SOURCE/workload/scripts/workGenLogs

echo $JOB_OUTPUT_DIR

rm -rf $JOB_OUTPUT_DIR$/progress-plot-job-*.txt

FILE_COUNT=`ls -1 $JOB_OUTPUT_DIR | wc -l`

echo $FILE_COUNT


#rm -rf $JOB_OUTPUT_DIR$/progress-plot-job-*.txt

for (( i = 0; i < $FILE_COUNT; i++ ))
do
    #rm -rf $JOB_OUTPUT_DIR/progress-plot-job-*.txt
    cat $JOB_OUTPUT_DIR/job-$i.txt | grep 'INFO mapreduce.Job:  map' | awk '{printf "%s\t%s\t%s\n", $2, $6, $8}' | sed "s/%//g" > $JOB_OUTPUT_DIR/progress-plot-job-$i.txt
done
