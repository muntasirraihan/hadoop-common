#!/usr/bin/env python
from __future__ import print_function, division

import json
import argparse
from subprocess import call
import os

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-j", "--json",
    default="run.json",
    help="json file to plot")
parser.add_argument("-t", "--title",
    default="Run",
    help="title of plot")
args = parser.parse_args()

with open(args.json) as f:
  run = json.load(f)

csv_data = ""
apps = sorted(run['appInfo'].keys())
# header line
csv_data += "time,"
csv_data += ",".join( ["Job %d Map,Job %d Reduce" % (i+1, i+1) for i in
  range(len(apps))] )
csv_data += "\n"

timestamps = [float(t) for t in sorted(run['appData'].keys())]
t0 = timestamps[0]
lastinfo = {}
for app in apps:
  lastinfo[app] = {
      "mapProgress": 0.0,
      "reduceProgress": 0.0,
      }
for t in sorted(run['appData'].keys()):
  csv_data += str(float(t) - t0) + ","
  for app in apps:
    try:
      info = run['appData'][t][app]
    except KeyError:
      info = None
    if info is None:
      info = lastinfo[app]
    lastinfo[app] = info
    csv_data += "%(mapProgress)f,%(reduceProgress)f," % info
  csv_data += "\n"

with open("run.csv", "w") as f:
  f.write(csv_data)
call(["./csv-lines.plt", "run", args.title, "time (s)", "Progress"])
#os.remove("run.csv")
