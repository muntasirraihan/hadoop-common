#!/usr/bin/env python

import pickle
import time

import argparse
parser = argparse.ArgumentParser(
    add_help=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-h", "--host",
    default="localhost",
    help="hostname where resourcemanager is running")
parser.add_argument("trace",
    help="pickled, estimated trace jobs")
args = parser.parse_args()

with open(args.trace) as f:
  jobs = pickle.load(f)
  waitTimes = pickle.load(f)

for job, wait in zip(jobs, waitTimes):
  job.run()
# wait times are written in seconds
  time.sleep(wait)
