#!/usr/bin/env python

from __future__ import print_function, division

import time
import json
import httplib2
import re

class URLResolver:
  def __init__(self, host):
    self.rm = "http://" + host + ":8088/ws/v1/cluster"
    self.history = "http://" + host + ":19888"
  def apps(self):
    return self.rm+ "/apps"
  def app(self, app_id):
    return self.rm+ "/proxy/" + app_id + "/ws/v1"
  def job(self, app_id):
    return self.app(app_id) + "/mapreduce/jobs"
  def history_info(self, app_id):
    job_id = re.sub("application", "job", app_id)
    return self.history + "/ws/v1/history/mapreduce/jobs/" + job_id
  def conf(self, app_id):
    return self.history_info(app_id) + "/conf"

class AppInfo:
  """ Interface to the cluster's metadata about applications. """
  def __init__(self, resolver):
    # private http client
    self._c = httplib2.Http()
    # timestamp-keyed, raw app data
    self.app_data = {}
    self.apps = set([])
    self.finished_apps = set([])
    self.url = resolver
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

  def update(self):
    now = time.time()
    apps_list = self._apps()
    if apps_list is None:
      return
    apps_list = [app for app in apps_list
        if app['id'] not in self.historical_apps]
    self.apps |= set([app['id'] for app in apps_list])
    info = {}
    for app in apps_list:
      this_info = self._job_info(app)
      info[app['id']] = this_info
      if this_info is not None:
        log_message = "app " + app['id'][-4:]
        log_message += " map: %(mapProgress)0.1f"
        log_message += " reduce: %(reduceProgress)0.1f"
        print(log_message % this_info)
      if app['state'] in ["FINISHED", "FAILED", "KILLED"]:
        self.finished_apps.add(app['id'])
    self.app_data[now] = info
  def dump(self, fp):
    """ Write out the info gathered so far.

    Takes a file-like object and writes a JSON representation of the app info
    to it.
    """
    app_info = {}
    for app_id in self.apps:
      app_info[app_id] = {}
      conf = self._job_conf(app_id)
      conf_map = {}
      for prop in conf['property']:
        if prop['name'] in ["mapreduce.job.deadline"]:
          conf_map[prop['name']] = prop['value']
      app_info[app_id]['conf'] = conf_map
      app_info[app_id]['finish_info'] = self._job_history_info(app_id)
    recorded_info = {
        'app_info': app_info,
        'app_data': self.app_data,
        }
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
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-j", "--json",
        default="run.json",
        help="file to output json to")
    parser.add_argument("-t", "--period",
        type=float,
        default=2,
        help="sampling period, in seconds"
        )
    parser.add_argument("-h", "--host",
        default="localhost",
        help="host name of resourcemanager/history server"
        )
    args = parser.parse_args()
    info = AppInfo(args.host)
    while not info.is_run_over():
      info.update()
      time.sleep(args.period)
    with open(args.json, 'w') as f:
      info.dump(f)
  main()

