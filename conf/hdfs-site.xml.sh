#!/bin/bash
NAMENODE="$1"
RESOURCEMANAGER="$2"
cat << EOF
<configuration>
  <property>
    <name>dfs.support.append</name>
    <value>true</value>
    <description>Enable or disable append. Defaults to false</description>
  </property>
</configuration>
EOF
