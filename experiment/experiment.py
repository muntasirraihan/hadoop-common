# Module for utilities braodly useful for experimentation utlities

from __future__ import print_function
from subprocess import call
from os.path import expanduser, expandvars
from math import sqrt
import json, pickle
import time

class GlobalConfig(object):
  """ Singleton reference to the global configuration. """
  config = None
  @classmethod
  def getConfig(cls):
    if cls.config is None:
      with open("global.json") as f:
        cls.config = json.load(f)
    return cls.config
  @classmethod
  def get(cls, key):
    return cls.getConfig()[key]

class Estimate(object):
  """ Track an estimated parameter with confidence measure. """
  def __init__(self, *args):
    """ Initialize with any number of values. """
    self.sum = 0
    self.sse = 0.0
    self.n = 0
    for v in args:
      self.add(v)
  def mean(self):
    return self.sum / self.n
  def stddev(self):
    return sqrt(self.sse / (self.n - 1))
  def add(self, v):
    oldmean = self.mean()
    self.sum += v
    self.n += 1
    self.sse += (v - oldmean) * (v - self.mean())

class Job(object):
  def __init__(self, epsilon, mapRatio):
    self.epsilon = epsilon
    self.mapRatio = mapRatio
  def run(self, *args):
    num = args[0]
    if len(args) < 2:
      deadline = 0
    else:
      deadline = args[1]
    dirs = GlobalConfig.get("dirs")
    script = expanduser(dirs["common"]) + "/workload/scripts/run-job.sh"
    args = [script]
    args.extend(["--deadline", deadline])
    jobParams = GlobalConfig.get("jobParams")
    args.extend(["--nummaps", jobParams["numMaps"]])
    args.extend(["--numreduces", jobParams["numReduces"]])
    args.extend(["--mapratio", self.mapRatio])
    args.extend(["--redratio", jobParams["reduceRatio"]])
    args.extend(["--jobs", num])
    args = [str(arg) for arg in args]
    call(args)
  def __repr__(self):
    return "e=%0.1f size=%0.0f" % (self.epsilon, self.mapRatio)

class EstimatedJob(Job):
  def __init__(self, job, runtimeMs):
    super(EstimatedJob, self).__init__(job.epsilon, job.mapRatio)
    self.runtime = runtimeMs
  def rel_deadline(self):
    return self.runtime * (1 + self.epsilon)
  def deadline(self):
    return float(time.time()) + self.rel_deadline()
  def run(self, *args):
    num = args[0]
    super(EstimatedJob, self).run(self, self.deadline(), num)
  def __repr__(self):
    return "e=%0.1f size=%0.0f runtimeMin=%0.1f" % \
      (self.epsilon, self.mapRatio, self.runtime / 60e3)

class Run(object):
  def __init__(self, jobs, delta, param):
    """
    jobs: list or tuple of two jobs
    delta: time difference submit(high deadline) - submit(low deadline)
    param: arbitrary parameter that characterizes this job
    """
    assert len(jobs) == 2
    self.jobs = jobs
    self.delta = delta
    self.param = param
  def __repr__(self):
    return repr(self.jobs) + " delta=%0.0f x=%0.0f" % (self.delta, self.param)
  def run(self, startnum):
    job0 = self.jobs[0]
    job1 = self.jobs[1]
    if job0.rel_deadline() > job1.rel_deadline():
      job0, job1 = job1, job0
    # deadline(job0) <= deadline(job1)
# delta is defined to be submit(high deadline) - submit(low deadline)
    if self.delta >= 0:
      job0.run(startnum)
      time.sleep(self.delta)
      job1.run(startnum+1)
    else:
      job1.run(startnum+1)
      time.sleep(-self.delta)
      job0.run(startnum)

class Experiment(object):
  def __init__(self, runs):
    self.runs = runs
  def write(self, fname):
    with open(fname, "w") as f:
      pickle.dump(self, f)
  def __repr__(self):
    return repr(self.runs)

def load(fname):
  """ Wrapper for pickle loading. """
  with open(fname, "r") as f:
    return pickle.load(f)

def clearHDFS():
  dirs = GlobalConfig.get("dirs")
  script = expanduser(dirs["target"]) + "/bin/hdfs"
  args = [script]
  args.extend("dfs -rm -r".split())
  args.append(expandvars("/user/$USER/out*"))
  call(args)
