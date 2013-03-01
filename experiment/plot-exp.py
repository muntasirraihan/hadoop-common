#!/usr/bin/env python
from __future__ import print_function, division

import experiment
from subprocess import call
import os
import os.path
from os.path import join

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
csv_data = exp.details["param"] + ",margin0,margin0_err,margin1,margin1_err\n"
for param, run in sorted(results.items()):
  csv_data += ("%f" % param) + ","
  margin_seconds = [margin_hat/1e3 for margin_hat in run["margins"]]
  margins = [(m_hat.mean(), m_hat.stddev()) for m_hat in margin_seconds]
  csv_data += ",".join(["%0.2f,%0.2f" % (mean, stddev)
    for mean, stddev in margins])
  csv_data += "\n"

if args.title is None:
  args.title = exp.details["name"].capitalize()
  if "epsilon" in exp.config:
    args.title += " - epsilon %0.2g" % exp.config["epsilon"]

plot_script = join(os.path.realpath(os.path.dirname(__file__)), "csv.plt")

with open("exp.csv", "w") as f:
  f.write(csv_data)
call([plot_script, "exp", args.title, exp.details["param"], "margin (s)"])
os.remove("exp.csv")
