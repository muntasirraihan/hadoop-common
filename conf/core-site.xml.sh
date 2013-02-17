#!/bin/bash
NAMENODE="$1"
RESOURCEMANAGER="$2"
cat << EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>

  <!-- file system properties -->

  <property>
    <name>fs.default.name</name>
    <value>hdfs://$NAMENODE:8020</value>
    <description>The name of the default file system.  Either the
      literal string "local" or a host:port for NDFS.
    </description>
    <final>true</final>
  </property>

  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-tmp/</value>
  </property>

</configuration>
EOF
