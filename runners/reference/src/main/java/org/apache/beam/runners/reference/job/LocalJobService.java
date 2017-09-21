/*
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

package org.apache.beam.runners.reference.job;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.common.runner.v1.JobApi;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceImplBase;

/**
 * An implementation of the Beam {@link JobServiceImplBase Job Service} which executes jobs using
 * the Reference Runner, storing job management details in memory.
 */
public class LocalJobService extends JobServiceImplBase {
  public static LocalJobService create() {
    return new LocalJobService();
  }

//  private final ConcurrentHashMap<String, PreJob> preparationsById;
//  private final ConcurrentHashMap<String, Job> jobsById;

  private LocalJobService() {}

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  @Override
  public void run(
      JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  @Override
  public void getState(JobApi.GetJobStateRequest request,
      StreamObserver<JobApi.GetJobStateResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }
}
