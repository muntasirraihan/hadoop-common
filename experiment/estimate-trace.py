#!/usr/bin/env python

import pickle

from os.path import splitext

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
  estimatedJob = job.estimate(args.host, args.numruns)
  jobs[jobNum] = estimatedJob

with open(args.output, "w") as f:
  pickle.dump(jobs, f)
  pickle.dump(waitTimes, f)

