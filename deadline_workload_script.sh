#!/bin/bash
clear

VERSION=0.23.3

HADOOP_HOME=$HOME/hadoop/hadoop-$VERSION-SNAPSHOT
HADOOP_SOURCE=$HOME/natjam/hadoop-common

#if [ ! -e $HADOOP_HOME/workload ]; then
#    ln -s $HADOOP_SOURCE/workload $HADOOP_HOME/workload
#fi

#rm -rf $HADOOP_HOME/workload
#mkdir $HADOOP_HOME/workload

#cp -r  $HADOOP_SOURCE/workload/* $HADOOP_HOME/workload/
#cp HDFSWrite.jar $HADOOP_HOME/
#cp randomwriter_conf.xsl $HADOOP_HOME/

# generate random input
#$HADOOP_HOME/bin/hadoop jar HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf $HADOOP_HOME/conf/randomwriter_conf.xsl -Dmapreduce.job.queuename=high workGenInputSmall

# clear output directories before running the test
$HADOOP_HOME/workload/scripts/run-test.sh