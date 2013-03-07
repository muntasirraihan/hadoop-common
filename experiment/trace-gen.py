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
# this is the the 1-hr trace; don't know how to parse the separate
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
        name = columns["id"]
        map_slots_millis = float(columns["map_slots_millis"])
        reduce_slots_millis = float(columns["reduce_slots_millis"])
        mapRatio = map_slots_millis/queueScaling["mapNormalize"]
        reduceRatio = reduce_slots_millis/queueScaling["reduceNormalize"]
        numMaps = int(columns["num_maps"])
        numReduces = int(columns["num_reduces"])
# these paramters are passed as arguments to run-jobs-script, using the keys as
# argument names
        params = {
            "queue": queue,
            "jobs": name,
            "mapratio": mapRatio,
            "redratio": reduceRatio,
            "nummaps": numMaps,
            "numreduces": numReduces,
            }
        jobs.append(experiment.TraceJob(params))
        if lastTime is not None:
# wait times should be written in seconds
          waitTimes.append( (submitTime - lastTime)/1e3)
        lastTime = submitTime

# make waitTimes and jobs the same length
    waitTimes.append(0)

    with open(args.output, "w") as f:
      pickle.dump(jobs, f)
      pickle.dump(waitTimes, f)
  main()
