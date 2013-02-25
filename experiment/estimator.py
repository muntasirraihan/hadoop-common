#!/usr/bin/env python

from __future__ import print_function, division

import experiment
import logger
import time

import argparse
parser = argparse.ArgumentParser(
    add_help=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-h", "--host",
    default="localhost",
    help="hostname where resourcemanager is running")
parser.add_argument("-f", "--exp",
    default="delta-exp.pickle",
    help="pickled experiment")
parser.add_argument("-o", "--output",
    default="delta-exp-estimated.pickle",
    help="output file for estimated experiment")
args = parser.parse_args()

# cache jobs that have already been estimated
estimatedJobs = {}

exp = experiment.load(args.exp)
jobnum = 0
experiment.clearOutput()
for runNum, run in enumerate(exp.runs):
  for jobNum, job in enumerate(run.jobs):
    if job in estimatedJobs:
      estimatedJob = estimatedJobs[job]
    else:
      info = logger.AppInfo(logger.URLResolver(args.host), measure=False)
      job.run(jobnum)
      while not info.is_run_over():
        info.update()
        time.sleep(4)
      # a hash with app keys is returned, but we only care about the single
      # submitted app
      appInfo = info.marshal()["appInfo"]
      apps = appInfo.keys()
      finishInfo = appInfo[apps[0]]["finishInfo"]
      runtimeMs = finishInfo["finishTime"] - finishInfo["startTime"]
      estimatedJob = experiment.EstimatedJob(job, runtimeMs)
      estimatedJobs[job] = estimatedJob
      jobnum += 1
    exp.runs[runNum].jobs[jobNum] = estimatedJob

exp.write(args.output)
