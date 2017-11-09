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
package org.apache.beam.runners.fnexecution.control;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Fn API control service which adds incoming SDK harness connections to a pool. */
public class FnApiControlClientPoolService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements BeamFnControlService {
  private static final Logger LOGGER = LoggerFactory.getLogger(FnApiControlClientPoolService.class);

  private final BlockingQueue<FnApiControlClient> clientPool;
  /**
   * The collection of clients that have been created by this {@link FnApiControlClientPoolService}.
   *
   * <p>Access to this field must by synchronized.
   */
  private final Set<FnApiControlClient> activeClients;

  private FnApiControlClientPoolService(BlockingQueue<FnApiControlClient> clientPool) {
    this.clientPool = clientPool;
    activeClients = new HashSet<>();
  }

  /**
   * Creates a new {@link FnApiControlClientPoolService} which uses a {@link SynchronousQueue} as
   * its pool of available clients.
   */
  public static FnApiControlClientPoolService synchronousPool() {
    return offeringClientsToPool(new SynchronousQueue<FnApiControlClient>());
  }

  /**
   * Creates a new {@link FnApiControlClientPoolService} which will enqueue and vend new SDK harness
   * connections.
   */
  @VisibleForTesting
  static FnApiControlClientPoolService offeringClientsToPool(
      BlockingQueue<FnApiControlClient> clientPool) {
    return new FnApiControlClientPoolService(clientPool);
  }

  /**
   * Called by gRPC for each incoming connection from an SDK harness, and enqueue an available SDK
   * harness client.
   *
   * <p>Note: currently does not distinguish what sort of SDK it is, so a separate instance is
   * required for each.
   */
  @Override
  public StreamObserver<BeamFnApi.InstructionResponse> control(
      StreamObserver<BeamFnApi.InstructionRequest> requestObserver) {
    LOGGER.info("Beam Fn Control client connected.");
    FnApiControlClient newClient = FnApiControlClient.forRequestObserver(this, requestObserver);
    synchronized (activeClients) {
      activeClients.add(newClient);
    }
    try {
      clientPool.put(newClient);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return newClient.asResponseObserver();
  }

  @Override
  public void close() throws Exception {
    Set<FnApiControlClient> toCleanUp;
    // Copy the active clients to ensure that we have a consistent view and are insulated from any
    // concurrent modifications.
    synchronized (activeClients) {
      toCleanUp = ImmutableSet.copyOf(activeClients);
    }
    for (FnApiControlClient activeClient : toCleanUp) {
      // Close all clients, swallowing any exceptions that closing those clients produce
      try {
        activeClient.close();
      } catch (Exception e) {
        LOGGER.warn(
            "Exception while closing down {} {}",
            FnApiControlClient.class.getSimpleName(),
            activeClient,
            e);
      }
    }
  }

  void clientClosed(FnApiControlClient fnApiControlClient) {
    synchronized (activeClients) {
      activeClients.remove(fnApiControlClient);
    }
  }

  @Override
  public SdkHarnessClient takeClient() throws InterruptedException {
    return SdkHarnessClient.usingFnApiClient(clientPool.take());
  }
}
