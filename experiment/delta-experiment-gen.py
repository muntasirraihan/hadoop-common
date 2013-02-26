#!/usr/bin/env python

from experiment import Job, Run, Experiment

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
    delta = minDelta
    while delta <= maxDelta:
      run = Run([self.job, self.job], delta, delta)
      runs.append(run)
      delta += stepsize
    return Experiment(runs)

if __name__ == "__main__":
  def main():
    import json
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--json",
        default="delta-exp.json",
        help="config file for experiment")
    parser.add_argument("-o", "--output",
        default="delta-exp.pickle",
        help="output file")
    args = parser.parse_args()
    with open(args.json) as f:
      config = json.load(f)
    expgen = DeltaExperimentGen(config)
    exp = expgen.build()
    exp.write(args.output)
  main()
