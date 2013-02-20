#!/bin/bash
clear

VERSION=0.23.3

HADOOP_HOME=$HOME/hadoop/hadoop-$VERSION-SNAPSHOT
HADOOP_SOURCE=$HOME/natjam/hadoop-common

# generate random input
$HADOOP_HOME/bin/hadoop jar $HADOOP_SOURCE/HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf $HADOOP_SOURCE/conf/randomwriter_conf.xsl -Dmapreduce.job.queuename=high workGenInputSmall
