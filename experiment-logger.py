#!/usr/bin/env python

from __future__ import print_function, division

import os
from os.path import expanduser, expanddvars, join
from subprocess import rmtree, copy
import time

import urllib, httplib2

rm_scheduler_apps_url = "http://localhost:8088/ws/v1/cluster/apps"

# we need to check the number of acive applications and then keep looping until the number is non-zero

h = httplib2.Http(".cache")

while True:

	resp, content = h.request(rm_scheduler_apps_url, "GET")

	print content
