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

package org.apache.hadoop.mapreduce.v2.app.release;

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ReleaseEvent extends AbstractEvent<Releaser.EventType> {

  private Resource releaseResource;
  private TaskId taskId;
  
  public ReleaseEvent(Resource releaseResource) {
    super(Releaser.EventType.RELEASE_RESOURCES);
    this.releaseResource = releaseResource;
  }
  
  public ReleaseEvent(TaskId taskId) {
    super(Releaser.EventType.RELEASED);
    this.taskId = taskId;
  }

  public Resource getReleaseResource() {
    return releaseResource;
  }
  
  public TaskId getTaskId() {
    return taskId;
  }
}