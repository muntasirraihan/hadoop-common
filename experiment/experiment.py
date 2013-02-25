# Module for utilities braodly useful for experimentation utlities

from __future__ import print_function
import json
from math import sqrt

class Estimate:
  """ Track an estimated parameter with confidence measure. """
  def __init__(self, *args, **kwargs):
    """ Initialize with a value or with json=string. """
    if len(args) >= 1:
      self.sum = 0
      self.sse = 0.0
      self.n = 0
      for v in args:
        self.add(v)
    else:
      o = json.loads(kwargs['json'])
      self.sum = o['sum']
      self.sse = o['sse']
      self.n = o['n']
  def mean(self):
    return self.sum / self.n
  def stddev(self):
    return sqrt(self.sse / (self.n - 1))
  def add(self, v):
    oldmean = self.mean()
    self.sum += v
    self.n += 1
    self.sse += (v - oldmean) * (v - self.mean())
  def marshal(self):
    return json.dumps({
      "sum": self.sum,
      "sse": self.sse,
      "n": self.n,
      })
