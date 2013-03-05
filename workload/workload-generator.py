#!/usr/bin/env python

#from __future__ import print_function, division

#import time
#import pickle

#from os import mkdir
#from os.path import splittext, exists

import optparse
parser = optparse.OptionParser()

parser.add_option('-P', '--pqueue', help='production queue arguments tuple <multiplier, map_normalize, reduce_normalize>', dest='prod_queue', action='store', nargs=3)

parser.add_option('-R', '--rqueue', help='research queue arguments tuple <multiplier, map_normalize, reduce_normalize>', dest='res_queue', action='store', nargs=3)

parser.add_option('-S', '--sleepnormalize', help='sleep normalization ratio, actual sleep is divided by this value to scale down', dest='sleep_normalize', action='store', nargs=1)

parser.add_option('-H', '--numhdfsinput', help='number of hdfs inputs', dest='num_hdfs_inputs', action='store', nargs=1)

parser.add_option('-I', '--input', help='input tsv filename path', dest='input_tsv', action='store', nargs=1)

parser.add_option('-E', '--run', help='run option: (analyze or generate)', dest='run_option', action='store', nargs=1)

(opts, args) = parser.parse_args()

print "production queue arguments: %s" % str(opts.prod_queue)

print "research queue arguments: %s" % str(opts.res_queue)

print "sleep normalize: %s" % str(opts.sleep_normalize)

print "num hdfs inputs: %s" % str(opts.num_hdfs_inputs)

print "input tsv file: %s" % str(opts.input_tsv)

print "run option: %s" % str(opts.run_option)
