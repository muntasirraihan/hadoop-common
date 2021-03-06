/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class Allocation {
  final List<Container> containers;
  final Resource resourceLimit;
  final List<ResourceRequest> releaseRequests;
  
  public Allocation(List<Container> containers, Resource resourceLimit) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.releaseRequests = new ArrayList<ResourceRequest>();
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      List<ResourceRequest> releaseRequests) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.releaseRequests = releaseRequests;
  }
  
  public List<Container> getContainers() {
    return containers;
  }

  public Resource getResourceLimit() {
    return resourceLimit;
  }
  
  public List<ResourceRequest> getReleaseRequests() {
    return releaseRequests;
  }
}
