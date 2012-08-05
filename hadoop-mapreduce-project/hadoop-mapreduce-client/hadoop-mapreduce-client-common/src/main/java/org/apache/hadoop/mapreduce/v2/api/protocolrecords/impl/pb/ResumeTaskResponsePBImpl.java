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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.ResumeTaskResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.ResumeTaskResponseProto;
import org.apache.hadoop.yarn.api.records.ProtoBase;


    
public class ResumeTaskResponsePBImpl extends ProtoBase<ResumeTaskResponseProto> implements ResumeTaskResponse {
  ResumeTaskResponseProto proto = ResumeTaskResponseProto.getDefaultInstance();
  ResumeTaskResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public ResumeTaskResponsePBImpl() {
    builder = ResumeTaskResponseProto.newBuilder();
  }

  public ResumeTaskResponsePBImpl(ResumeTaskResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResumeTaskResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResumeTaskResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}  
