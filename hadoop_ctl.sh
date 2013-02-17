#!/bin/bash
clear

VERSION=0.23.3

# this is where the hadoop tar.gz distribution is
HADOOP_COMMON_DIR=$HOME/natjam/hadoop-common
HADOOP_SOURCE_DIR=$HADOOP_COMMON_DIR/hadoop-dist/target

# this where the hadoop folder is going to be
HADOOP_TARGET_DIR=$HOME/hadoop

HADOOP_HOME=$HADOOP_TARGET_DIR/hadoop-$VERSION-SNAPSHOT
export HADOOP_HOME

echo $HADOOP_HOME

HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME

echo $HADOOP_MAPRED_HOME

HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME

echo $HADOOP_COMMON_HOME

HADOOP_HDFS=$HADOOP_HOME
export HADOOP_HDFS

echo $HADOOP_HDFS

YARN_HOME=$HADOOP_HOME
export YARN_HOME

echo $YARN_HOME

HADOOP_CONF_DIR=$HADOOP_HOME/conf
export HADOOP_CONF_DIR
echo $HADOOP_CONF_DIR

YARN_CONF_DIR=$HADOOP_CONF_DIR
export YARN_CONF_DIR
echo $YARN_CONF_DIR

# this is the hadoop_conf source
HADOOP_CONF_SOURCE_DIR=.


#rm -rf $HADOOP_TARGET_DIR
#mkdir $HADOOP_TARGET_DIR
#chmod a+w $HADOOP_TARGET_DIR
tar -xzf $HADOOP_SOURCE_DIR/hadoop-0.23.3-SNAPSHOT.tar.gz -C $HADOOP_TARGET_DIR
cp $HADOOP_SOURCE_DIR/hadoop-0.23.3-SNAPSHOT/share/hadoop/mapreduce/hadoop-mapreduce-examples-0.23.3-SNAPSHOT.jar $HADOOP_TARGET_DIR/hadoop-0.23.3-SNAPSHOT/hadoop-examples.jar

ln -s $HADOOP_COMMON_DIR/conf $HADOOP_HOME/conf

$HADOOP_HOME/sbin/hadoop-daemon.sh stop namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh stop datanode
$HADOOP_HOME/sbin/yarn-daemon.sh stop resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager


rm -rf /tmp/dfs
rm -rf /tmp/nm-local-dirs
rm -rf /tmp/nm-log-dirs
rm -rf /tmp/yarn-logs
rm -rf /tmp/logs

mkdir /tmp/nm-local-dirs
chmod a+w /tmp/nm-local-dirs

mkdir /tmp/nm-log-dirs
chmod a+w /tmp/nm-log-dirs

$HADOOP_HOME/bin/hadoop namenode -format

$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
