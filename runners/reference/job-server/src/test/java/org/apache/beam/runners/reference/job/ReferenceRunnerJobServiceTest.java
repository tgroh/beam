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

import com.google.protobuf.Struct;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceStub;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ReferenceRunnerJobService}.
 */
@RunWith(JUnit4.class)
public class ReferenceRunnerJobServiceTest {
  private static final String JOB_SERVICE_NAME = "job_service";

  @Rule
  public TemporaryFolder stagingBase = new TemporaryFolder();
  @Rule
  public TemporaryFolder scratch = new TemporaryFolder();

  private InProcessServerFactory serverFactory = InProcessServerFactory.create();
  private ReferenceRunnerJobService service;

  private GrpcFnServer<ReferenceRunnerJobService> server ;
  private final ManagedChannel channel = InProcessChannelBuilder.forName(JOB_SERVICE_NAME).build();

  @Before
  public void setup() throws IOException {
    service = ReferenceRunnerJobService.create(stagingBase.getRoot(), serverFactory);
    server =
        GrpcFnServer.create(
            service,
            ApiServiceDescriptor.newBuilder().setUrl(JOB_SERVICE_NAME).build(),
            serverFactory);
  }

  @After
  public void teardown() throws Exception {
    server.close();
  }

  @Test
  public void testPrepareJob() {
    JobServiceBlockingStub stub = JobServiceGrpc.newBlockingStub(channel);

    PrepareJobResponse prepareResponse =
        stub.prepare(
            PrepareJobRequest.newBuilder()
                .setJobName("test_job")
                .setPipeline(Pipeline.getDefaultInstance())
                .setPipelineOptions(Struct.getDefaultInstance())
                .build());

    ArtifactStagingServiceStub stagingStub =
        ArtifactStagingServiceGrpc.newStub(
            InProcessChannelBuilder.forName(prepareResponse.getArtifactStagingEndpoint().getUrl())
                .build());
  }
}
