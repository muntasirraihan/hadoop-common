#!/usr/bin/env python

import pickle

import sys
from os.path import splitext
import experiment
from experiment import showTime, remainingTime
import time

CACHE_PATH = experiment.CACHE_DIR + ("/%s-trace-runtime-estimates.pickle" %
    experiment.hostname())

runtimeEstimates = experiment.loadEstimates(CACHE_PATH)

import argparse
parser = argparse.ArgumentParser(
    add_help=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-f", "--trace",
    default="trace-jobs.pickle",
    help="input trace jobs file")
parser.add_argument("-h", "--host",
    default="localhost",
    help="hostname where resourcemanager is running")
parser.add_argument("-n", "--numruns",
    type=int,
    default=1,
    help="number of runs to use for estimate")
parser.add_argument("-o", "--output",
    help="output estimated trace file (default is <input>-est.pickle)")
parser.add_argument("--help",
    action="store_true",
    help="print out usage")
args = parser.parse_args()

if args.help:
  parser.print_help()
  sys.exit(0)

if args.output is None:
  base, ext = splitext(args.trace)
  args.output = base + "-est" + ext

with open(args.trace) as f:
  jobs = pickle.load(f)
  waitTimes = pickle.load(f)

unestimatedJobs = 0
estimatedJobs = 0
for job in jobs:
  if job.size() in runtimeEstimates:
    estimatedJobs += 1
  else:
    unestimatedJobs += 1
jobsDone = 0

startTime = time.time()
for jobNum, job in enumerate(jobs):
  if job.size() in runtimeEstimates:
    runtimeEstimate = runtimeEstimates[job.size()]
  else:
    runtimeEstimate = job.estimate(args.host, args.numruns)
    runtimeEstimates[job.size()] = runtimeEstimate
    experiment.saveEstimates(runtimeEstimates, CACHE_PATH)
    jobsDone += 1
    print("%d/%d jobs done" % (estimatedJobs + jobsDone, len(jobs)))
    print("~%s remaining" %
        showTime(remainingTime(startTime, jobsDone, unestimatedJobs)))
  job.runtime = runtimeEstimate
  jobs[jobNum] = job

with open(args.output, "w") as f:
  pickle.dump(jobs, f)
  pickle.dump(waitTimes, f)

