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

package org.apache.beam.runners.fnexecution.environment;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.SettableFuture;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;

/**
 * A {@link EnvironmentManager} which manages a single Docker Container.
 *
 * <p>The container is initialized on the first call to {@link #getEnvironment(Environment)}. Future
 * calls will fail if there is more than one environment.
 */
public class SingletonDockerEnvironmentManager implements EnvironmentManager {
  public static EnvironmentManager forServers(
      GrpcFnServer<SdkHarnessClientControlService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer) {
    return new SingletonDockerEnvironmentManager(
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer);
  }

  private final SettableFuture<DockerContainerEnvironment> process = SettableFuture.create();

  private final GrpcFnServer<SdkHarnessClientControlService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;

  private SingletonDockerEnvironmentManager(
      GrpcFnServer<SdkHarnessClientControlService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer) {
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
  }

  @Override
  public RemoteEnvironment getEnvironment(Environment environment) throws Exception {
    if (process.isDone()) {
      DockerContainerEnvironment dockerContainerEnvironment = process.get();
      checkArgument(
          environment.getUrl().equals(dockerContainerEnvironment.getEnvironment().getUrl()),
          "A %s must only be queried for a single %s. Existing %s, Argument %s",
          SingletonDockerEnvironmentManager.class.getSimpleName(),
          Environment.class.getSimpleName(),
          dockerContainerEnvironment.getEnvironment().getUrl(),
          environment.getUrl());
    }
    // TODO: Generate this with an IDGenerator, or any other appropriate source.
    String environmentId = Long.toString(-123);
    // This is managed by the runner - it could be managed actively, but currently is just in /tmp
    Path workerPersistentDirectory = Files.createTempDirectory("worker_persistent_directory");
    String containerImage = environment.getUrl();
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();
    // An arbitrary path for the harness to write scratch files
    String semiPersistentDirectory = "/tmp/runner/semi_persistent_dir";
    ProcessBuilder builder =
        new ProcessBuilder(
            "docker",
            "run",
            "-d" /* run the container in detached mode, which will print the container ID. */,
            "-v",
            String.format("%s:%S", workerPersistentDirectory, semiPersistentDirectory),
            containerImage,
            String.format("--id=%s", environmentId),
            String.format("--logging_endpoint=%s", loggingEndpoint),
            String.format("--artifact_endpoint=%s", artifactEndpoint),
            String.format("--provision_endpoint=%s", provisionEndpoint),
            String.format("--control_endpoint=%s", controlEndpoint),
            String.format("--semi_persist_dir=%s", semiPersistentDirectory));

    Process subprocess = builder.inheritIO().start();
    process.set(
        DockerContainerEnvironment.forProcessAndClient(
            environment, subprocess, controlServiceServer.getService().getClient()));
    return process.get();
  }
}
