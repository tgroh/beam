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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.common.runner.v1.JobApi.PrepareJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.PrepareJobResponse;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceStub;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} capable of executing an arbitrary Beam {@link Pipeline} using the Beam
 * portability framework.
 *
 * <p>The {@link ULRunner} spins up an external process (either Java or a Docker container) which
 * provides a Job API endpoint. It then communicates with that process via GRPC, providing a Java
 * {@link PipelineResult} to interact with.
 */
public class ULRunner extends PipelineRunner<ULResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ULRunner.class);

  private final PipelineOptions options;
  private final JobServiceStub stub;
  private final Channel jobApiChannel;

  /**
   * Create a new {@link ULRunner} from the provided options.
   */
  @SuppressWarnings("unused")
  public static ULRunner fromOptions(PipelineOptions options) {
    return new ULRunner(options);
  }

  private ULRunner(PipelineOptions options) {
    this.options = options;
    this.jobApiChannel = startJobApiEndpoint(options);
    this.stub = JobServiceGrpc.newStub(jobApiChannel);
  }

  @Override
  public ULResult run(Pipeline pipeline) {
    StreamObserver<PrepareJobResponse> stageFilesAndRunObserver =
        new StreamObserver<PrepareJobResponse>() {
          @Override
          public void onNext(PrepareJobResponse value) {
            // TODO: Send back enough information to create an artifact staging stub and stage all
            // of the appropriate classpath files.
            value.getPreparationId();
          }

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };
    stub.prepare(
        PrepareJobRequest.newBuilder()
            .setJobName(generateJobName())
            .setPipeline(PipelineTranslation.toProto(pipeline))
            .setPipelineOptions(PipelineOptionsTranslation.toProto(options))
            .build(),
        stageFilesAndRunObserver);
    throw new UnsupportedOperationException();
  }

  private String generateJobName() {
    return String.format("ReferenceRunner-JavaPipeline-%s", ThreadLocalRandom.current().nextLong());
  }

  private Channel startJobApiEndpoint(PipelineOptions options) {
    ULOptions myOptions = options.as(ULOptions.class);
    switch (myOptions.getJobServerType()) {
      case LOCAL_PROCESS:
        return getChannelForExistingServer(myOptions.getJobServerPort());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown Job API Endpoint Type: %s", myOptions.getJobServerType()));
    }
  }

  private ManagedChannel getChannelForExistingServer(int port) {
    return ManagedChannelBuilder.forAddress("127.0.0.1", port)
        .usePlaintext(true)
        .directExecutor()
        .build();
  }
}
