#!/usr/bin/env python

from __future__ import print_function, division

import os
from os.path import expanduser, expandvars, join
from subprocess import call
from shutil import rmtree, copy
import time

import sys

env = {}
env["common"] = expanduser("~/natjam/hadoop-common")
env["src"] = "%(common)s/hadoop-dist/target" % env
env["target"] = expanduser("~/hadoop")
env["ver"] = "0.23.3-SNAPSHOT"
env["h-ver"] = "hadoop-%(ver)s" % env
env["h-home"] = "%(target)s/%(h-ver)s" % env
env["tmp"] = "/tmp"

def cleartree(path):
  """ Clear everything inside a path without deleting the directory. """
  for fname in os.listdir(path):
    if fname == "." or fname == "..":
      continue
    file_path = os.path.join(path, fname)
    try:
      if os.path.isfile(file_path) or os.path.islink(file_path):
        os.unlink(file_path)
      else:
        rmtree(file_path)
    except Exception:
      pass

class Command:
  """ Represent a command that is part of this script. """
  # track registration order with a class counter
  num = 0
  def __init__(self, fn):
    self.fn = fn
    self.name = fn.__name__
    self.doc = fn.__doc__
    self.num = Command.num
    Command.num += 1
  # order commands by registration order
  def __le__(self, other):
    return self.num <= other.num
  def __str__(self):
    if self.doc is not None:
      return "%s: %s" % (self.name, self.doc)
    return self.name
  def run(self):
    print("running " + self.name)
    self.fn()

commands = {}

def command(f):
  """ Decorator that registers the function as a command. """
  commands[f.__name__] = Command(f)
  return f

@command
def help():
  """ Show usage. """
  print("Usage: " + sys.argv[0] + " <cmd>")
  print("Supported commands:")
  for command_object in sorted(commands.itervalues()):
    print(command_object)

@command
def compile():
  """ Compile and package the hadoop distribution. """
  commandline = "mvn3 package -Pdist -DskipTests -Dtar"
  commandline += " -Dmaven.javadoc.skip=true"
  call(commandline.split(), cwd="%(common)s" % env)

@command
def extract():
  """ Extract the compiled hadoop source,
  overwriting what is in the target directory. """
# remove what's inside the target folder
  cleartree("%(target)s/%(h-ver)s" % env)
# extract a packaged tar (create with mvn package; details on wiki)
  call(["tar", "-xzf", "%(src)s/%(h-ver)s.tar.gz" % env, "-C", env["target"]])
# symlink conf to repository conf
  os.symlink("%(common)s/conf" % env, "%(h-home)s/conf" % env)
# remove existing default conf
  rmtree("%(target)s/%(h-ver)s/etc" % env)
# copy over our compiled example jar for convenient access
  example_jar = "%(src)s/%(h-ver)s/share/hadoop/mapreduce/" % env
  example_jar += "hadoop-mapreduce-examples-%(ver)s.jar" % env
  copy(example_jar, "%(target)s/%(h-ver)s/hadoop-examples.jar" % env)

@command
def clear_logs():
  """ Clear out log directories. """
  cleartree("%(tmp)s/yarn-logs" % env)

@command
def setup_logs():
  """ Ensure log directories exist """
  for logname in ["nm-local-dirs", "nm-log-dirs", "yarn-logs"]:
    logdir = ("/%(tmp)s/" % env) + logname
    if not os.path.isdir(logdir):
      os.makedirs(logdir)

def daemon_script(system, startstop, component):
  """ Run a particular daemon script.

  system is the particular subsystem to run (eg, hadoop or yarn)
  startstop is either the string "start" or "stop"
  component is the component to start or stop
  """
  script = join("%(h-home)s/sbin" % env, "%s-daemon.sh" % system)
  call([script, startstop, component])

@command
def start_hdfs():
  daemon_script("hadoop", "start", "namenode")
  daemon_script("hadoop", "start", "datanode")
@command
def stop_hdfs():
  daemon_script("hadoop", "stop", "datanode")
  daemon_script("hadoop", "stop", "namenode")
@command
def restart_hdfs():
  stop_hdfs()
  time.sleep(4)
  start_hdfs()

@command
def clear_input():
  """ Clear the input folders in HDFS. """
  call(["%(target)s/%(h-ver)s/bin/hdfs" % env, "dfs", "-rm", "-r", "-f", expandvars("/user/$USER/workGenInput*")])

@command
def clear_output():
  """ Clear the output folders (/output*) in HDFS. """
  call(["%(target)s/%(h-ver)s/bin/hdfs" % env, "dfs",
    "-rm", "-r", "-f", "/output*"])
  call(["%(target)s/%(h-ver)s/bin/hdfs" % env, "dfs",
    "-rm", "-r", "-f", expandvars("/user/$USER/out*")])

@command
def refresh_queues():
  """ Refresh Queue configurations. Remember to update capacityscheduler.xml"""
  call(["%(target)s/%(h-ver)s/bin/yarn" % env, "rmadmin", "-refreshQueues"])

@command
def format_hdfs():
  """ Format HDFS. """
  call(["%(target)s/%(h-ver)s/bin/hadoop" % env, "namenode", "-format"])
@command
def start_nm():
  daemon_script("yarn", "start", "nodemanager")
def start_yarn():
  daemon_script("yarn", "start", "resourcemanager")
  start_nm()
def stop_yarn():
  daemon_script("yarn", "stop", "nodemanager")
  time.sleep(2)
  daemon_script("yarn", "stop", "resourcemanager")
def restart_yarn():
  stop_yarn()
  time.sleep(2)
  start_yarn()
@command
def start():
  """ Start YARN """
  start_yarn()
@command
def stop():
  """" Stop YARN """
  stop_yarn()
@command
def restart():
  restart_yarn()
@command 
def start_history():
  daemon_script("mr-jobhistory", "start", "historyserver")
@command
def stop_history():
  daemon_script("mr-jobhistory", "stop", "historyserver")
@command
def flush():
  """ Clear out everything - output, logs - and shutdown YARN. """
  stop_yarn()
  clear_output()
  clear_logs()
@command
def sleep():
  """ Sleep for 4 seconds - for chaining together commands. """
  time.sleep(4)

if len(sys.argv) < 2:
  help()
  sys.exit(1)
cmds = []
for cmd_name in sys.argv[1:]:
  if cmd_name not in commands:
    print("'%s' is not a known command" % cmd_name)
    help()
    sys.exit(1)
  cmds.append(commands[cmd_name])

for cmd in cmds:
  cmd.run()
