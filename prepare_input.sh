#!/bin/bash
clear

EXPECTED_ARGS=1

if [ $# -ne $EXPECTED_ARGS ]
then
    echo "Usage: ./prepare_input <clustername>"
    echo "cluster_name = natjam | bec"
    exit 1
fi

VERSION=0.23.3

CLUSTER_NAME=$1

HADOOP_HOME=/mnt/hadoop/hadoop-$VERSION-SNAPSHOT
HADOOP_SOURCE=/proj/ISS/scheduling

# generate random input
$HADOOP_HOME/bin/hadoop jar $HADOOP_SOURCE/HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf $HADOOP_SOURCE/conf_$CLUSTER_NAME/randomwriter_conf.xsl -Dmapreduce.job.queuename=high workGenInputSmall
