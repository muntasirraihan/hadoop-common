#!/usr/bin/env python
from __future__ import print_function, division

import experiment
from subprocess import call
import os

import argparse
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("results",
    help="pickled experiment results file")
parser.add_argument("-t", "--title",
    default=None,
    help="plot title (default: experiment name)")
args = parser.parse_args()

exp, results = experiment.loadResults(args.results)
# header row
csv_data = exp.details["param"] + ",margin0,margin1\n"
for param, run in results.items():
  csv_data += (exp.details["param_f"] % param) + ","
  margins = [margin_hat.mean() for margin_hat in run["margins"]]
  csv_data += ",".join(["%0.0f" % (margin/1e3) for margin in margins])
  csv_data += "\n"

if args.title is None:
  args.title = exp.details["name"].capitalize()

with open("exp.csv", "w") as f:
  f.write(csv_data)
call(["./csv.plt", "exp", args.title, exp.details["param"], "margin (s)"])
#os.remove("exp.csv")
