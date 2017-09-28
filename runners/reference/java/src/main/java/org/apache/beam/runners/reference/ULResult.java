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

package org.apache.beam.runners.reference;

import io.grpc.Channel;
import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobState.JobStateType;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * A result of running a pipeline with the {@link ULRunner}.
 *
 * <p>Communicates with the {@link JobServiceGrpc Job Service} that the controlling process
 * maintains.
 */
public class ULResult implements PipelineResult {
  private final JobServiceBlockingStub runnerStub;
  private final String jobId;

  private ULResult(String jobId, Channel jobServiceChannel) {
    this.jobId = jobId;
    this.runnerStub = JobServiceGrpc.newBlockingStub(jobServiceChannel);
  }

  @Override
  public State getState() {
    return toJavaState(
        runnerStub.getState(GetJobStateRequest.newBuilder().setJobId(jobId).build()).getState());
  }

  @Override
  public State cancel() throws IOException {
    return toJavaState(
        runnerStub.cancel(CancelJobRequest.newBuilder().setJobId(jobId).build()).getState());
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }

  private State toJavaState(JobStateType state) {
    switch (state) {
      default:
        throw new UnsupportedOperationException();
    }
  }
}
