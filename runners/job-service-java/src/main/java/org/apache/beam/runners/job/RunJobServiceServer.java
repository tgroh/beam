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

package org.apache.beam.runners.job;

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.beam.runners.core.construction.PipelineManager;

/**
 * A program that runs a {@link PipelineManagerJobService} with a Stub {@link PipelineManager}.
 */
public class RunJobServiceServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    checkArgument(
        args.length == 2,
        "Usage: %s [%s] [Port]",
        RunJobServiceServer.class.getSimpleName(),
        PipelineManager.class.getSimpleName());
    PipelineManagerJobService service = new PipelineManagerJobService(null);
    int port = Integer.parseInt(args[0]);
    Server server = ServerBuilder.forPort(port).addService(service).build();
    server.start();
    server.awaitTermination();
  }
}
