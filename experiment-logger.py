#!/usr/bin/env python

from __future__ import print_function, division

import time
import json
import httplib2

class AppInfo:
  """ Interface to the ResourceManager's metadata about applications. """
  BASE_URL = "http://localhost:8088/ws/v1/cluster"
  APPS_URL = BASE_URL + "/apps"
  @classmethod
  def app_url_base(cls, app):
    """ Get the base proxy url for the app object. """
    return "http://localhost:8088/proxy/" + app['id'] + "/ws/v1"
  @classmethod
  def app_job_url(cls, app):
    """ Get the job info url for the app object. """
    return cls.app_url_base(app) + "/mapreduce/jobs"
  def __init__(self):
    # private http client
    self._c = httplib2.Http()
    # timestamp-keyed, raw app data
    self.app_data = {}
    self.apps = set([])
    self.finished_apps = set([])
  def _apps(self):
    """ Get the list of applications from the ResourceManager. """
    resp, content = self._c.request(AppInfo.APPS_URL)
# check for error response
    if resp['status'] != '200':
      return None
    apps = json.loads(content)['apps']
    if apps is None:
      return None
    return apps['app']
  def _job_info(self, app):
    """ Get the status of an application's job.
    
    Fetches the info for the first job in the application from the AM or
    JobHistory server.
    """
    try:
      print(AppInfo.app_job_url(app))
      resp, content = self._c.request(AppInfo.app_job_url(app))
    except Exception:
      return None
    if resp['status'] != '200':
      return None
    # return only the first job in the application since we do not consider
    # DAGs of jobs (yet)
    try:
      return json.loads(content)['jobs']['job'][0]
    except ValueError as e:
      print(e)
      print(content[:200])

  def update(self):
    now = time.time()
    apps_list = self._apps()
    if apps_list is None:
      return
    self.apps |= set([app['id'] for app in apps_list])
    info = {}
    for app in apps_list:
      info[app['id']] = self._job_info(app)
      if app['state'] in ["FINISHED", "FAILED", "KILLED"]:
        self.finished_apps.add(app['id'])
    self.app_data[now] = info
  def dump(self, fp):
    """ Write out the info gathered so far.

    Takes a file-like object and writes a JSON representation of the app info to it.
    """
    app_info = {
        'apps': list(self.apps),
        'app_info': self.app_data,
        }
    json.dump(app_info, fp)
  def is_experiment_over(self):
    total = len(self.apps)
    finished = len(self.finished_apps)
    return total == finished and total > 0

if __name__ == "__main__":
  import sys
  def main():
    info = AppInfo()

    while not info.is_experiment_over():
      info.update()
      time.sleep(3)
    info.dump(sys.stdout)
  main()

