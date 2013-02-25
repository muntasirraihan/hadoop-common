#!/usr/bin/env bash

VERSION=0.23.3

HADOOP_COMMON=$HOME/natjam/hadoop-common
HADOOP_HOME=$HOME/hadoop/hadoop-$VERSION-SNAPSHOT

CURRDIR="`pwd`"
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#defaults
ARGS="jar $HADOOP_COMMON/workload/WorkGenYarn.jar org.apache.hadoop.examples.WorkGenYarn -conf $HADOOP_COMMON/conf/workGenKeyValue_conf.xsl -libjars $HADOOP_COMMON/workload/WorkGenYarn.jar"
BINDIR="$HADOOP_HOME/bin"
OUTDIR="workGenLogs"

#parse args
arg_type="none"

if [ -z "$1" ]; then
  echo "Usage $0 --deadline <deadline> --mapratio <mapratio> --redratio <redratio> --nummaps <nummaps> --numreduces <numreduces> --jobs <jobnum>"
fi

cd $SCRIPTDIR
./run-jobs-script.sh \
 --bindir $BINDIR --outdir $OUTDIR \
 --args $ARGS \
 --redratio-distribution uniform --suspend-strategy shortest \
 --hdfs-input workGenInputSmall \
 --interval 1 --duration 8 \
 --verbose true \
 --hdfs-input-num 100 \
 --queue low $@

cd $CURRDIR
