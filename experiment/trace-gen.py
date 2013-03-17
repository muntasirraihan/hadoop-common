#!/usr/bin/env python

from __future__ import print_function, division
import experiment
import json, pickle
import random
import math

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
# this is for the 1-hr trace; don't know how to parse the separate
# research/production traces
column_indices = {
    "id": 0,
    "queue": 1,
    "submission_time": 2,
    "map_slots_millis": 3,
    "reduce_slots_millis": 4,
    "num_maps": 5,
    "num_reduces": 6,
    }
def getColumn(name, l):
  split = l.split("\t")
  idx = column_indices[name]
  return split[idx]
def getColumns(l):
  columns = {}
  for name in column_indices.iterkeys():
    columns[name] = getColumn(name, l)
  return columns

if __name__ == "__main__":
# protect variables from outermost scope
  def main():
    with open(args.json) as f:
      scaling = json.load(f)

    jobs = []
    waitTimes = []

    with open(args.input) as f:
      lastTime = None
      for line in f:
        columns = getColumns(line)
        submitTime = int(columns["submission_time"])
        queue = columns["queue"]
# we're only considering jobs from a specific queue
        if not queue == QUEUE:
          continue
        queueScaling = scaling["queues"][queue]
        multiplier = queueScaling["multiplier"]
        name = columns["id"]
        map_slots_millis = float(columns["map_slots_millis"])
        reduce_slots_millis = float(columns["reduce_slots_millis"])
        mapRatio = map_slots_millis/queueScaling["mapNormalize"]
        reduceRatio = reduce_slots_millis/queueScaling["reduceNormalize"]
        numMaps = int(columns["num_maps"]) * multiplier
        numMaps = int(math.ceil(numMaps))
        numReduces = int(columns["num_reduces"]) * multiplier
        numReduces = int(math.ceil(numReduces))
# skip this job, doesn't involve any computation
        if numMaps == 0 and numReduces == 0:
          continue
        if numMaps == 0:
          # Simulate no mapping
          numMaps = 1
          mapRatio = 0.0
        if numReduces == 0:
# Simulate no reducing
          numReduces = 1
          reduceRatio = 0.0
        epsilonModel = queueScaling["epsilon"]
        if epsilonModel["distribution"] not in ["uniform"]:
          raise ValueError("unsupported distribution: " +
              epsilonModel["distribution"])
        epsilon = random.uniform(epsilonModel["min"], epsilonModel["max"])
# These paramters are passed as arguments to run-jobs-script, using the keys as
# argument names. Epsilon is treated specially and converted to an appropriate
# deadline parameter.
        params = {
            #"queue": queue,
            "queue": "high",
            "jobs": name,
            "mapratio": mapRatio,
            "redratio": reduceRatio,
            "nummaps": numMaps,
            "numreduces": numReduces,
            "epsilon": epsilon,
            }
        print(params)
        jobs.append(experiment.TraceJob(params))
        sleepNormalize = queueScaling["sleepNormalize"]
        if lastTime is not None:
# wait times should be written in seconds
          waitTimes.append( (submitTime - lastTime)/1e3/sleepNormalize)
        lastTime = submitTime

# make waitTimes and jobs the same length
    waitTimes.append(0)

    with open(args.output, "w") as f:
      pickle.dump(jobs, f)
      pickle.dump(waitTimes, f)
  main()
