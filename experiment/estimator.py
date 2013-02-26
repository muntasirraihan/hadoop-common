#!/usr/bin/env python

from __future__ import print_function, division

import experiment
import logger
import time
import pickle

from os.path import splitext
import argparse
parser = argparse.ArgumentParser(
    add_help=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-h", "--host",
    default="localhost",
    help="hostname where resourcemanager is running")
parser.add_argument("exp",
    help="pickled experiment")
parser.add_argument("-n", "--numruns",
    type=int,
    default=3,
    help="number of runs to use for estimate")
parser.add_argument("-o", "--output",
    help="output file for estimated experiment")
args = parser.parse_args()

if args.output is None:
  base, ext = splitext(args.exp)
  args.output = base + "-est" + ext

# cache jobs that have already been estimated
try:
  runtimeEstimates = experiment.load("caches/runtime_estimates.pickle")
except Exception:
  runtimeEstimates = {}

def saveEstimates():
  with open("caches/runtime_estimates.pickle", "w") as f:
    pickle.dump(runtimeEstimates, f)

exp = experiment.load(args.exp)
jobnum = 0
experiment.clearHDFS()
for runNum, run in enumerate(exp):
  for jobNum, job in enumerate(run.jobs):
    if job.size() in runtimeEstimates:
      runtimeEstimate = runtimeEstimates[job.size()]
    else:
      runtimeEstimate = experiment.Estimate()
      for i in range(args.numruns):
        print("estimate %d" % (i+1))
        info = logger.AppInfo(args.host, measure=False)
        submitTime = float(time.time())
        job.run(jobnum)
        while not info.is_run_over():
          info.update()
          time.sleep(4)
        # a hash with app keys is returned, but we only care about the single
        # submitted app
        appInfo = info.appInfo()
        apps = appInfo.keys()
        finish = appInfo[apps[0]]["finishInfo"]
        runtimeMs = finish["finishTime"] - finish["startTime"]
        print("accept time of %0.2fs" % (finish["startTime"]/1e3 - submitTime))
        print("runtime of %0.2fmin" % (runtimeMs/60e3))
        runtimeEstimate.add(runtimeMs)
        jobnum += 1
      runtimeEstimates[job.size()] = runtimeEstimate
    estimatedJob = experiment.EstimatedJob(job, runtimeEstimate)
    exp.runs[runNum].jobs[jobNum] = estimatedJob

saveEstimates()

exp.write(args.output)
