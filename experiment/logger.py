#!/usr/bin/env python

from __future__ import print_function, division

import time
import json
import httplib2
import re
import sys
import subprocess

class URLResolver:
  def __init__(self, host):
    self.rm = "http://" + host + ":8088"
    self.history = "http://" + host + ":19888"
  def apps(self):
    return self.rm + "/ws/v1/cluster/apps"
  def app(self, app_id):
    return self.rm + "/proxy/" + app_id + "/ws/v1"
  def job(self, app_id):
    return self.app(app_id) + "/mapreduce/jobs"
  def history_info(self, app_id):
    job_id = re.sub("application", "job", app_id)
    return self.history + "/ws/v1/history/mapreduce/jobs/" + job_id
  def conf(self, app_id):
    return self.history_info(app_id) + "/conf"

def progressBar(perc, width):
# use up 2 spaces for the delimiters
  width -= 2
  completed = int(perc * width + 0.5)
  bar = "["
  bar += "=" * completed
  bar += (width - completed) * " "
  bar += "]"
  return bar

class AppInfo:
  """ Interface to the cluster's metadata about applications. """
  def __init__(self, host, **kwargs):
    if 'measure' in kwargs:
      self.measure = kwargs['measure']
    else:
      self.measure = True
    # private http client
    self._c = httplib2.Http()
    # timestamp-keyed, raw app data
    self.appData = {}
# cached end-of-run app info
    self._appInfo = None
    self.apps = set([])
    self.finished_apps = set([])
    self.url = URLResolver(host)
    apps = self._apps()
    if apps is not None:
      self.historical_apps = set([app['id'] for app in apps])
    else:
      self.historical_apps = set([])
  def _getJSON(self, url):
    try:
      resp, content = self._c.request(url)
    except Exception:
      return None
    if resp['status'] != '200':
      return None
    try:
      return json.loads(content)
    except ValueError:
      return None
  def _apps(self):
    """ Get the list of applications from the ResourceManager. """
    apps = self._getJSON(self.url.apps())
    if apps is None:
      return None
    apps = apps['apps']
    if apps is None:
      return None
    return apps['app']
  def _job_info(self, app):
    """ Get the status of an application's job.
    
    Fetches the info for the first job in the application from the AM; only
    works if job is still running.
    """
    jobs = self._getJSON(self.url.job(app['id']))
    if jobs is None:
      return
    # return only the first job in the application since we do not consider
    # DAGs of jobs (yet)
    return jobs['jobs']['job'][0]
  def _job_conf(self, app_id):
    """ Get the status for a job, specified by application id. """
    conf = self._getJSON(self.url.conf(app_id))
    if conf is None:
      return None
    return conf['conf']
  def _job_history_info(self, app_id):
    info = self._getJSON(self.url.history_info(app_id))
    if info is None:
      return None
    return info['job']
  def update(self, **kwargs):
    if "log" in kwargs:
      logStatus = kwargs["log"]
    else:
      logStatus = True
    apps_list = self._apps()
    if apps_list is None:
      return
    apps_list = [app for app in apps_list
        if app['id'] not in self.historical_apps]
    self.apps |= set([app['id'] for app in apps_list])
    info = {}
    subprocess.call(["clear"])
    for app in sorted(apps_list, key=lambda app: app['id']):
      this_info = self._job_info(app)
      info[app['id']] = this_info
      if this_info is not None and logStatus:
        for key in ["map", "reduce"]:
          this_info[key + "Bar"] = progressBar(this_info[key + "Progress"]/100, 50)
        log_prefix = "app " + app['id'][-4:]
        log_message = log_prefix
        log_message += " map: %(mapBar)s %(mapProgress)0.1f\n"
        log_message += " " * len(log_prefix) + " red: %(reduceBar)s %(reduceProgress)0.1f"
        print(log_message % this_info)
      if app['state'] in ["FINISHED", "FAILED", "KILLED"]:
        self.finished_apps.add(app['id'])
    if self.measure:
      now = time.time()
      self.appData[now] = info
  def appInfo(self):
    if self._appInfo is None:
      appInfo = {}
      for app_id in sorted(self.apps):
        info = {}
        conf = self._job_conf(app_id)
        conf_map = {}
        if conf is not None:
          for prop in conf['property']:
            if prop['name'] in [
                "mapreduce.job.deadline",
                "mapreduce.job.queuename"]:
              conf_map[prop['name']] = prop['value']
        info['conf'] = conf_map
        info['finishInfo'] = self._job_history_info(app_id)
        if "mapreduce.job.deadline" not in conf_map:
          scheduled = True
          margin = float('inf')
        else:
          finishTime = info['finishInfo']['finishTime']
          deadline = int(conf_map["mapreduce.job.deadline"])
          margin = deadline - finishTime
          scheduled = margin > 0
        info['scheduled'] = scheduled
        info['margin'] = margin
        if "mapreduce.job.queuename" in conf_map:
          info['queue'] = conf_map["mapreduce.job.queuename"]
        else:
          info['queue'] = ""
        appInfo[app_id] = info
      self._appInfo = appInfo
    return self._appInfo
  def orderedInfo(self):
    appInfo = self.appInfo()
    apps = sorted(appInfo.iteritems(),
        key=lambda kv: kv[1]["finishInfo"]["name"])
    return [kv[1] for kv in apps]
  def scheduledPerc(self):
    appInfo = self.appInfo()
    scheduled = 0
    total = len(appInfo)
    for info in appInfo.itervalues():
      if info["scheduled"]:
        scheduled += 1
    return scheduled/total
  def marshal(self):
    recorded_info = {
        'appInfo': self.appInfo(),
        'appData': self.appData,
        }
    return recorded_info
  def dump(self, fp):
    """ Write out the info gathered so far.

    Takes a file-like object and writes a JSON representation of the app info
    to it.
    """
    recorded_info = self.marshal()
    json.dump(recorded_info, fp)
  def is_run_over(self):
    total = len(self.apps)
    finished = len(self.finished_apps)
    return total == finished and total > 0

if __name__ == "__main__":
# use a main function to hide variables
  def main():
    import argparse
    parser = argparse.ArgumentParser(
        add_help=False,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-j", "--json",
        default="run.json",
        help="file to output json to")
    parser.add_argument("-t", "--period",
        type=float,
        default=2,
        help="sampling period, in seconds"
        )
    parser.add_argument("-f", "--forever",
        action="store_true",
        help="run forever, even after all jobs have finished")
    parser.add_argument("-p", "--print",
        choices=["deadline", "time"],
        default="time",
        help="what to print at end")
    parser.add_argument("-h", "--host",
        default="localhost",
        help="host name of resourcemanager/history server"
        )
    parser.add_argument("--help",
        action="store_true",
        help="print help")
    args = parser.parse_args()
    if args.help:
      parser.print_help()
      sys.exit(0)
    info = AppInfo(args.host)
    try:
      while args.forever or not info.is_run_over():
        info.update()
        time.sleep(args.period)
    except KeyboardInterrupt:
      pass
    finally:
      with open(args.json, 'w') as f:
        info.dump(f)
        appInfo = info.appInfo()
        for app_id in sorted(info.apps):
          if args.print == "margin":
            if 'margin' in appInfo[app_id]:
              margin = appInfo[app_id]['margin']
              print("margin: %0.0fs" % (margin/1e3))
          if args.print == "time":
            finishInfo = appInfo[app_id]['finishInfo']
            if finishInfo is not None:
              queueName = appInfo[app_id]['queue']
              time_taken = finishInfo['finishTime'] - finishInfo['startTime']
              print("[queue: %s] time: %0.0fs" % (queueName, time_taken/1e3))

  main()

