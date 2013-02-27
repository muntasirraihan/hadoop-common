#!/usr/bin/env python

from __future__ import print_function, division

import experiment
import json

import argparse
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("results",
    help="pickled experiment results file")
parser.add_argument("--key", default="all", choices=["all", "exp", "global"])
args = parser.parse_args()

exp, results = experiment.loadResults(args.results)

output = {
    "exp": {
      exp.details["name"] : exp.config
      }, 
    "global": exp.globalConfig
    }

if args.key != "all":
  output = output[args.key]

print(json.dumps(output, indent=4))
