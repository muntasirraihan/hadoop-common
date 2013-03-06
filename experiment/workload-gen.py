#!/usr/bin/env python

from __future__ import print_function, division
import experiment
import json, pickle

import argparse
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-j", "--json",
    default="scaling.json",
    help="json file with scaling specification")
parser.add_argument("input",
    help="input tsv file name")
parser.add_argument("-o", "--output",
    default="trace-jobs.pickle",
    help="output file name")
args = parser.parse_args()

QUEUE = "production"

# 0-based column indices
# None for unknown positions
column_indices = {
    "queue": None,
    "submission_time": 4,
    "map_slots_millis": None,
    "red_slots_millis": None,
    "num_maps": None,
    "num_reduces": None,
    }
def getColumn(name, l):
  split = l.split("\t")
  idx = column_indices[name]
  return split[idx]

with open(args.json) as f:
  scaling = json.load(f)

jobs = []
waitTimes = []

with open(args.input) as f:
  lastTime = None
  for line in f:
    submitTime = int(getColumn("submission_time", line))
    params = {
        }
    jobs.append(experiment.TraceJob(params))
    if lastTime is not None:
      waitTimes.append(submitTime - lastTime)
    lastTime = submitTime

with open(args.output, "w") as f:
  pickle.dump(jobs, f)
  pickle.dump(waitTimes, f)
