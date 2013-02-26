#!/usr/bin/env python

from experiment import Job, Run, Experiment

def floatRange(start, step, stop):
  v = start
  while v <= stop:
    yield v
    v += step

class DeltaExperimentGen(object):
  def __init__(self, config):
    self.config = config
    epsilon = config["epsilon"]
    mapRatio = config["mapRatio"]
    self.job = Job(epsilon, mapRatio)
  def build(self):
    minDelta = self.config["min"]
    maxDelta = self.config["max"]
    steps = self.config["steps"]
    runs = []
    stepsize = (maxDelta - minDelta)/steps
    for delta in floatRange(minDelta, stepsize, maxDelta):
      run = Run([self.job, self.job], delta, delta)
      runs.append(run)
    return Experiment(runs)

class BetaExperimentGen(object):
  def __init__(self, config):
    self.config = config
    epsilon = config["baseJob"]["epsilon"]
    mapRatio = config["baseJob"]["mapRatio"]
    self.job0 = Job(epsilon, mapRatio)
    self.delta = config["delta"]
  def build(self):
    minBeta = self.config["min"]
    maxBeta = self.config["max"]
    steps = self.config["steps"]
    runs = []
    stepsize = (maxBeta - minBeta)/steps
    for beta in floatRange(minBeta, stepsize, maxBeta):
      job1 = Job(self.job0.epsilon, self.job0.mapRatio * beta)
      run = Run([self.job0, job1], self.delta, beta)
      runs.append(run)
    return Experiment(runs)

if __name__ == "__main__":
  import sys
  def main():
    import json
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--json",
        default="exp.json",
        help="config file for experiments")
    parser.add_argument("name",
        help="name of experiment")
    parser.add_argument("-o", "--output",
        default=None,
        help="output file")
    args = parser.parse_args()

    with open(args.json) as f:
      config = json.load(f)
    if args.name not in config:
      print("experiment %s not found" % args.name)
      return 1
    expconfig = config[args.name]
    if args.name == "delta":
      expgen = DeltaExperimentGen(expconfig)
    elif args.name == "beta":
      expgen = BetaExperimentGen(expconfig)
    else:
      print("generator for experiment %s not found" % args.name)
      return 1
    exp = expgen.build()
    if args.output is None:
      args.output = args.name + ".pickle"
    exp.write(args.output)
    return 0
  sys.exit(main())
