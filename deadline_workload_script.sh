#!/bin/bash
clear

HADOOP_SOURCE=$HOME/natjam/hadoop-common

# clear output directories before running the test
$HADOOP_SOURCE/workload/scripts/run-test.sh
