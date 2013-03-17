#!/usr/bin/env python
# Analyze traces to determine analytically some properties that are helpful for
# tuning.

from __future__ import print_function, division

import experiment
import math
import pickle
import sys

def runtimes(jobs):
  """ Generator for job runtimes. """
  for job in jobs:
    yield job.runtime.mean() / 1e3

def runtimeStats(jobs, waitTimes):
  table = [["stat", "value"]]
  times = [t for t in runtimes(jobs)]
  mu = math.fsum(times)/len(jobs)
  table.append(["mean", "%0.2fs" % mu])
  errors = [(t - mu) ** 2 for t in times]
  sigma = math.fsum(errors) / (len(errors) - 1)
  table.append(["stddev","%0.2fs" % math.sqrt(sigma)])
  return table

def runtimeSeries(jobs, waitTimes):
  table = [["runtime"]]
  for runtime in runtimes(jobs):
    table.append(["%0.2f" % runtime])
  return table

def deadlineInversions(jobs, waitTimes):
  table = [["inversions"]]
  inversions = 0
  lastStart = 0
# Optimisitic start times of jobs (converted to ms); assumes all jobs can run
# immediately
  startTimes = []
  for waitTime in waitTimes:
    startTimes.append(lastStart)
    lastStart += waitTime * 1e3
  for idx1 in range(len(jobs)):
    for idx2 in range(idx1+1, len(jobs)):
      job1 = jobs[idx1]
      job2 = jobs[idx2]
      deadline1 = startTimes[idx1] + job1.rel_deadline()
      deadline2 = startTimes[idx2] + job2.rel_deadline()
      if deadline2 < deadline1:
        inversions += 1
  table.append([inversions])
  return table

import argparse
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("trace",
    help="pickled trace file")
parser.add_argument("-a", "--analysis",
    default="runtime-stats",
    choices=["runtime-stats", "runtime-series", "deadline-inversions"])
parser.add_argument("-o", "--output",
    default="-",
    help="output file for tsv data")
args = parser.parse_args()

dispatchTable = {
    "runtime-stats": runtimeStats,
    "runtime-series": runtimeSeries,
    "deadline-inversions": deadlineInversions
    }

with open(args.trace) as f:
  jobs = pickle.load(f)
  waitTimes = pickle.load(f)

table = dispatchTable[args.analysis](jobs, waitTimes)

if args.output == "-":
  outF = sys.stdout
else:
  outF = open(args.output, "w")

for row in table:
  outF.write("\t".join([str(d) for d in row]))
  outF.write("\n")
