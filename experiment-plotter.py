#!/usr/bin/env python
from __future__ import print_function, division

import json
import argparse
from subprocess import call
import os

parser = argparse.ArgumentParser()
parser.add_argument("-j", "--json",
    default="experiment.json",
    help="json file to plot")
args = parser.parse_args()

with open(args.json) as f:
  experiment = json.load(f)

csv_data = ""
apps = sorted(experiment['app_info'].keys())
# header line
csv_data += "time,"
csv_data += ",".join( ["j%d m,j%d r" % (i, i) for i in range(len(apps))] )
csv_data += "\n"

timestamps = [float(t) for t in sorted(experiment['app_data'].keys())]
t0 = timestamps[0]
lastinfo = {}
for app in apps:
  lastinfo[app] = {
      "mapProgress": 0.0,
      "reduceProgress": 0.0,
      }
for t in sorted(experiment['app_data'].keys()):
  csv_data += str(float(t) - t0) + ","
  for app in apps:
    try:
      info = experiment['app_data'][t][app]
    except KeyError:
      info = None
    if info is None:
      info = lastinfo[app]
    lastinfo[app] = info
    csv_data += "%(mapProgress)f,%(reduceProgress)f," % info
  csv_data += "\n"

with open("experiment.csv", "w") as f:
  f.write(csv_data)
call(["gnuplot", "experiment.plt"])
os.remove("experiment.csv")
