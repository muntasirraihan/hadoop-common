#!/usr/bin/env python

import pickle

from os.path import splitext
import experiment

CACHE_PATH = experiment.CACHE_DIR + ("/%s-trace-runtime-estimates.pickle" %
    experiment.hostname())

runtimeEstimates = experiment.loadEstimates(CACHE_PATH)

import argparse
parser = argparse.ArgumentParser(
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
args = parser.parse_args()

if args.output is None:
  base, ext = splitext(args.exp)
  args.output = base + "-est" + ext

with open(args.trace) as f:
  jobs = pickle.load(f)
  waitTimes = pickle.load(f)

for jobNum, job in enumerate(jobs):
  if job.size() in runtimeEstimates:
    runtimeEstimate = runtimeEstimates[job.size()]
  else:
    runtimeEstimate = job.estimate(args.host, args.numruns)
    runtimeEstimates[job.size()] = runtimeEstimate
  job.runtime = runtimeEstimate
  jobs[jobNum] = job

experiment.saveEstimates(runtimeEstimates, CACHE_PATH)

with open(args.output, "w") as f:
  pickle.dump(jobs, f)
  pickle.dump(waitTimes, f)

