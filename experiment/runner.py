#!/usr/bin/env python
# vim: ft=python

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
totalRuns = len(exp) * args.numruns
startTime = time.time()
for run in exp:
  print("running %s" % run)
  s_hat = experiment.Estimate()
  margins_hat = [experiment.Estimate() for i in xrange(2)]
# record the complete final info from the history server for each run just in
# case it's useful later.
  finalInfo = []
  for i in range(args.numruns):
    info = logger.AppInfo(args.host, measure=False)
    run.run(runNum)
    runNum += 1
    updateNum = 0
    while not info.is_run_over():
      info.update(log=(updateNum % 5 == 0))
      time.sleep(2)
      updateNum += 1
    finalInfo.append(info.appInfo())
    s = info.scheduledPerc()
    s_hat.add(s)
    infos = info.orderedInfo()
    for appInfo, margin_hat in zip(infos, margins_hat):
      margin_hat.add(appInfo["margin"])
    print("scheduled %0.1f jobs" % s)
    print("~%s remaining" %
        showTime(remainingTime(startTime, runNum, totalRuns)))
  results[run.param] = {
      "s": s_hat,
      "margins": margins_hat,
      "infos": finalInfo,
      }
print("total time: %s" % showTime(time.time() - startTime))
with open(args.output, "w") as f:
  pickle.dump(exp, f)
  pickle.dump(results, f)
