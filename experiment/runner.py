#!/usr/bin/env python

import logger
import experiment
from experiment import showTime, remainingTime
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
    help="pickled, estimated experiment")
parser.add_argument("-n", "--numruns",
    type=int,
    default=1,
    help="number of runs to use for estimate")
parser.add_argument("-o", "--output",
    default=None,
    help="output file for experiment results")
args = parser.parse_args()

if args.output is None:
  base, ext = splitext(args.exp)
  args.output = base + "-results.pickle"

exp = experiment.load(args.exp)
experiment.clearHDFS()
results = {}
runNum = 0
totalRuns = len(exp)
startTime = time.time()
for run in exp:
  print("running %s" % run)
  s_hat = experiment.Estimate()
  for i in range(args.numruns):
    info = logger.AppInfo(args.host, measure=False)
    run.run(runNum)
    runNum += 1
    updateNum = 0
    while not info.is_run_over():
      logStatus = (updateNum % 5 == 0)
      info.update(log=logStatus)
      time.sleep(2)
      updateNum += 1
    s = info.scheduledPerc()
    s_hat.add(s)
    print("scheduled %0.1f jobs" % s)
    print("~%s remaining" %
        showTime(remainingTime(startTime, runNum + 1, totalRuns)))
  results[run.param] = s_hat
with open(args.output, "w") as f:
  pickle.dump(results, f)
