#!/usr/bin/env python

from experiment import Job, Run, Experiment

def floatRange(start, stop, step):
  """ Iterates from start to stop in sizes of step.

  Supports floats for all parameters.
  """
  v = start
  while v <= stop:
    yield v
    v += step

def stepSize(start, stop, steps):
  return (stop - start)/(steps - 1)

def configStep(config):
  """ Iterates over the step given in a config.

  Requires the space to be specified as the keys min/max and steps
  Returns an iterator.
  """
  minParam = config["min"]
  maxParam = config["max"]
  steps = config["steps"]
  stepsize = stepSize(minParam, maxParam, steps)
  return floatRange(minParam, maxParam, stepsize)

# global dictionary of generator classes
generators = {}
def experiment(cls):
  """ Register an experiment generator class. """
# generators are known in this frontend by their name
  name = cls.details["name"]
  generators[name] = cls
  return cls

@experiment
class DeltaExperimentGen(object):
  details = {
      "name": "delta",
      "param": "delta",
      "param_f": "%d",
      }
  def __init__(self, config):
    self.config = config
    epsilon = config["epsilon"]
    mapRatio = config["mapRatio"]
    self.job = Job(epsilon, mapRatio)
  def build(self):
    runs = []
    for delta in configStep(self.config):
      run = Run([self.job, self.job], delta, delta)
      runs.append(run)
    return Experiment(runs, self.__class__.details, self.config)

@experiment
class BetaExperimentGen(object):
  details = {
      "name": "beta",
      "param": "beta",
      "param_f": "%0.2f",
      }
  def __init__(self, config):
    self.config = config
    epsilon = config["epsilon"]
    mapRatio = config["mapRatio"]
    self.job0 = Job(epsilon, mapRatio)
    self.delta = config["delta"]
  def build(self):
    runs = []
    for beta in configStep(self.config):
      job1 = Job(self.job0.epsilon, self.job0.mapRatio * beta)
      run = Run([self.job0, job1], self.delta, beta)
      runs.append(run)
    return Experiment(runs, self.__class__.details, self.config)

@experiment
class EpsilonExperimentGen(object):
  details = {
      "name": "epsilon",
      "param": "epsilon",
      "param_f": "%0.2g",
      }
  def __init__(self, config):
    self.config = config
    self.mapRatio = config["mapRatio"]
    self.delta = config["delta"]
  def build(self):
    runs = []
    for epsilon in configStep(self.config):
      job = Job(epsilon, self.mapRatio)
      run = Run([job, job], self.delta, epsilon)
      runs.append(run)
    return Experiment(runs, self.__class__.details, self.config)

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
        choices=generators.keys(),
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
    expgen = generators[args.name](expconfig)
    exp = expgen.build()
    if args.output is None:
      args.output = args.name + ".pickle"
    exp.write(args.output)
    return 0
  sys.exit(main())
