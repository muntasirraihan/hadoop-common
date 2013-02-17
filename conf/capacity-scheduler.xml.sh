#!/bin/bash
NAMENODE="$1"
RESOURCEMANAGER="$2"
cat << EOF
<!-- To change queue properties (without shutdown): $ bin/yarn rmadmin -refreshQueues -->
<!-- To submit mapreduce job to queue, use e.g. -Dmapreduce.job.queue.name=research -->
<configuration>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default,inter</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.inter.capacity</name>
    <value>99</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.inter.queues</name>
    <value>med,high</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.inter.high.capacity</name>
    <value>99</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.inter.med.capacity</name>
    <value>1</value>
  </property>
  <!-- yarn.scheduler.capacity.research.maximum-capacity is another variable to look at -->
  <!--
  <property>
    <name>yarn.scheduler.capacity.root.research.maximum-capacity</name>
    <value>20</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.production.maximum-capacity</name>
    <value>20</value>
  </property>
  -->
  <!-- user-limit-factor allows a SINGLE user to become very elastic (see documentation) -->
  <property>
    <name>yarn.scheduler.capacity.root.research.user-limit-factor</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.production.user-limit-factor</name>
    <value>100</value>
  </property>
  <!-- Maximum percent of resources in the cluster which can be used to run application masters - controls number of concurrent running applications. Specified as a float - ie 0.5 = 50%. Default is 10%. -->
  <property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>1.0</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.preempt.on</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.preempt.suspend.strategy</name>
    <value>edf</value>
    <!-- random, probabilistic, least-resources, most-resources, edf -->
  </property>
  <property>
    <name>yarn.scheduler.capacity.preempt.suspend</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.preempt.kill-ms</name>
    <value>12000</value>
    <!-- 500, 12000 -->
  </property>
  <property>
    <name>yarn.scheduler.capacity.preempt.expire-ms</name>
    <value>2000</value>
  </property>
</configuration>
EOF
