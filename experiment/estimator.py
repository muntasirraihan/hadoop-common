#!/usr/bin/env python

from __future__ import print_function, division

import experiment

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

CACHE_PATH = experiment.CACHE_DIR + ("/%s-runtime-estimates.pickle" %
    experiment.hostname())

# cache jobs that have already been estimated
runtimeEstimates = experiment.loadEstimates(CACHE_PATH)

exp = experiment.load(args.exp)
experiment.clearHDFS()
for runNum, run in enumerate(exp):
  for jobNum, job in enumerate(run.jobs):
    if job.size() in runtimeEstimates:
      runtimeEstimate = runtimeEstimates[job.size()]
    else:
      runtimeEstimate = job.estimate(args.host, args.numruns)
      runtimeEstimates[job.size()] = runtimeEstimate
    estimatedJob = experiment.EstimatedJob(job, runtimeEstimate)
    exp.runs[runNum].jobs[jobNum] = estimatedJob

experiment.saveEstimates(runtimeEstimates, CACHE_PATH)

exp.write(args.output)
