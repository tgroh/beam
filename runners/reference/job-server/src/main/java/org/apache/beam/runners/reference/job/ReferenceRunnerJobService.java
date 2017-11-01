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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.artifact.local.LocalFileSystemArtifactStagerService;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceImplBase;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The ReferenceRunner uses the portability framework to execute a Pipeline on a single machine. */
public class ReferenceRunnerJobService extends JobServiceImplBase implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRunnerJobService.class);

  private final File stagingBase;
  private final ServerFactory serverFactory;
  private final ConcurrentMap<String, PendingJob> pendingJobs = new ConcurrentHashMap<>();

  public static ReferenceRunnerJobService create(File stagingBase, ServerFactory serverFactory) {
    return new ReferenceRunnerJobService(stagingBase, serverFactory);
  }

  private ReferenceRunnerJobService(File stagingBase, ServerFactory serverFactory) {
    this.stagingBase = stagingBase;
    this.serverFactory = serverFactory;
  }

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
    try {
      LOG.trace("{} {}", PrepareJobResponse.class.getSimpleName(), request);
      // TODO: Improve this factoring. The JobService should route this to a Job-Preparer rather
      // than creating the file itself.
      File stagingLocation =
          new File(
              stagingBase,
              String.format(
                  "%s-%s-%s",
                  request.getJobName(), "staging", ThreadLocalRandom.current().nextInt()));
      if (!stagingLocation.mkdirs()
          && !(stagingLocation.exists() && stagingLocation.isDirectory())) {
        throw Status.INTERNAL
            .withDescription(
                String.format(
                    "Could not create local staging location at %s",
                    stagingLocation.getAbsolutePath()))
            .asRuntimeException();
      }
      LocalFileSystemArtifactStagerService stager =
          LocalFileSystemArtifactStagerService.withRootDirectory(stagingLocation);
      GrpcFnServer<LocalFileSystemArtifactStagerService> stagerServer =
          GrpcFnServer.allocatePortAndCreate(stager, ServerFactory.createDefault());
      PendingJob job =
          PendingJob.builder()
              .setArtifactStagingLocation(stagingLocation)
              .setStagerServer(stagerServer)
              .build();
      if (pendingJobs.putIfAbsent(request.getJobName(), job) != null) {
        throw Status.ALREADY_EXISTS
            .withDescription(
                String.format("Already have a pending job for name %s", request.getJobName()))
            .asRuntimeException();
      }

      responseObserver.onNext(
          PrepareJobResponse.newBuilder()
              .setPreparationId(request.getJobName())
              .setArtifactStagingEndpoint(stagerServer.getApiServiceDescriptor())
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      responseObserver.onError(e);
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  @Override
  public void run(
      JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {
    LOG.trace("{} {}", RunJobRequest.class.getSimpleName(), request);
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }

  @Override
  public void getState(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    responseObserver.onError(
        Status.NOT_FOUND
            .withDescription(String.format("Unknown Job ID %s", request.getJobId()))
            .asException());
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
    responseObserver.onError(
        Status.NOT_FOUND
            .withDescription(String.format("Unknown Job ID %s", request.getJobId()))
            .asException());
  }

  @Override
  public void close() {
    // TODO: Close active Jobs, etc
  }
}
