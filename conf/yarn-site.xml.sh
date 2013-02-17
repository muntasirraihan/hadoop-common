#!/bin/bash
NAMENODE="$1"
RESOURCEMANAGER="$2"
cat << EOF
<?xml version="1.0"?>
<configuration>

<!-- Site specific YARN configuration properties -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce.shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>$RESOURCEMANAGER:8025</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>$RESOURCEMANAGER:8030</value>
    </property>
<!-- Use CapacityScheduler -->
    <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>$RESOURCEMANAGER:8040</value>
    </property>
    <property>
        <name>yarn.nodemanager.local-dirs</name>
        <value>/tmp/hadoop-tmp/nm-local-dirs</value>
    </property>
    <property>
        <name>yarn.nodemanager.log-dirs</name>
        <value>/tmp/hadoop-tmp/nm-log-dirs</value>
    </property>
<!--
    <property>
        <name>yarn.nodemanager.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.nodemanager.remote-app-log-dir</name>
        <value>/tmp/hadoop-tmp/remote-app-log-dir</value>
    </property>
-->
    <property>
        <name>yarn.resourcemanager.resume-local-only</name>
        <value>false</value>
    </property>

<!-- medium containers -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>12288</value>
    </property>

</configuration>
EOF
