#!/usr/bin/env python

from __future__ import print_function, division

import time
import json
import httplib2
import re

class AppInfo:
  """ Interface to the ResourceManager's metadata about applications. """

  # URL to the resource manager's REST API.
  RM_URL = "http://localhost:8088"
# URL to the history server
  HISTORY_URL = "http://localhost:19888"
  BASE_URL = RM_URL + "/ws/v1/cluster"
  APPS_URL = BASE_URL + "/apps"
  @classmethod
  def app_url_base(cls, app):
    """ Get the base proxy url for the app object. """
    return cls.RM_URL + "/proxy/" + app['id'] + "/ws/v1"
  @classmethod
  def app_job_url(cls, app):
    """ Get the job info url for the app object. """
    return cls.app_url_base(app) + "/mapreduce/jobs"
  @classmethod
  def app_history_base_url(cls, app_id):
    job_id = re.sub("application", "job", app_id)
    return cls.HISTORY_URL + "/ws/v1/history/mapreduce/jobs/" + job_id
  @classmethod
  def app_conf_url(cls, app_id):
    return cls.app_history_base_url(app_id) + "/conf"
  @classmethod
  def app_history_info_url(cls, app_id):
    return cls.app_history_base_url(app_id)
  def __init__(self):
    # private http client
    self._c = httplib2.Http()
    # timestamp-keyed, raw app data
    self.app_data = {}
    self.apps = set([])
    self.finished_apps = set([])
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
    apps = self._getJSON(AppInfo.APPS_URL)
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
    jobs = self._getJSON(AppInfo.app_job_url(app))
    if jobs is None:
      return
    # return only the first job in the application since we do not consider
    # DAGs of jobs (yet)
    return jobs['jobs']['job'][0]
  def _job_conf(self, app_id):
    """ Get the status for a job, specified by application id. """
    conf = self._getJSON(AppInfo.app_conf_url(app_id))
    if conf is None:
      return None
    return conf['conf']
  def _job_history_info(self, app_id):
    info = self._getJSON(AppInfo.app_history_info_url(app_id))
    if info is None:
      return None
    return info['job']

  def update(self):
    now = time.time()
    apps_list = self._apps()
    if apps_list is None:
      return
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
  def is_experiment_over(self):
    total = len(self.apps)
    finished = len(self.finished_apps)
    return total == finished and total > 0

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument("json", help="file to output json to")
  args = parser.parse_args()
  def main():
    info = AppInfo()

    while not info.is_experiment_over():
      info.update()
      time.sleep(2)
    with open(args.json, 'w') as f:
      info.dump(f)
  main()

