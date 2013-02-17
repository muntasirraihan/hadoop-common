#!/bin/bash
NAMENODE="$1"
RESOURCEMANAGER="$2"
cat << EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.history.server.http.address</name>
    <value>$NAMENODE:51111</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>1024</value> <!-- changed from 1536 10/5/2012 -->
  </property>
  <!-- Added these properties so killing enabled jobs don't fail -->
  <property>
    <name>mapreduce.job.maxtaskfailures.per.tracker</name>
    <value>30</value>
  </property>
  <property>
    <name>mapreduce.task.skip.start.attempts</name>
    <value>20</value>
  </property>
  <property>
    <name>mapreduce.map.maxattempts</name>
    <value>40</value>
  </property>
  <property>
    <name>mapreduce.reduce.maxattempts</name>
    <value>40</value>
  </property>
</configuration>
EOF