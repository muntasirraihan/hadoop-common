# Module for utilities braodly useful for experimentation utlities

from __future__ import print_function
import math

class Estimate:
  """ Track an estimated parameter with confidence measure. """
  def __init__(self, x):
    self.sum = x
# sum of squared error
    self.sse = 0.0
    self.n = 1
  def mean(self):
    return self.sum / self.n
  def stddev(self):
    return math.sqrt(self.sse / (self.n - 1))
  def add(self, v):
    oldmean = self.mean()
    self.sum += v
    self.n += 1
    self.sse += (v - oldmean) * (v - self.mean())


