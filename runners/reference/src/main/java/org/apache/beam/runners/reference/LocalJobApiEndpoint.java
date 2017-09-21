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

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.reference.job.LocalJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by tgroh on 9/21/17. */
public class LocalJobApiEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(LocalJobApiEndpoint.class);

  /**
   * Runs a Local Job API server. The first argument will be used as the port of the server that
   * contains a Job Service implementation.
   */
  public static void main(String[] args) {
    checkArgument(args.length == 1, "Usage: LocalJobApiEndpoint [port]");
    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    int port = address.getPort();
    LocalJobService jobService = LocalJobService.create();
    Server server = ServerBuilder.forPort(0).addService(jobService).build();
    try {
      server.start();
    } catch (IOException e) {
      LOG.info("Job API Server failed to start on port {}", port, e);
      return;
    }
    LOG.info("Job API Server started on port {}", port);
    try {
      server.awaitTermination();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Job API Server on port {} interrupted, shutting down", port);
      shutdown(server, e);
    }
  }

  private static void shutdown(Server server, Exception cause) {
    server.shutdown();
    try {
      // Wait a bit to try to let any active process clean up
      server.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e1) {
      cause.addSuppressed(e1);
    }
    server.shutdownNow();
  }
}
