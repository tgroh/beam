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

import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Fn API control service which adds incoming SDK harness connections to a pool. */
public class FnApiControlClientPoolService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements FnService {
  private static final Logger LOGGER = LoggerFactory.getLogger(FnApiControlClientPoolService.class);

  private final BlockingQueue<FnApiControlClient> clientPool;
  private final ConcurrentMap<StreamObserver<InstructionResponse>, FnApiControlClient> clients;

  private FnApiControlClientPoolService(BlockingQueue<FnApiControlClient> clientPool) {
    this.clientPool = clientPool;
    clients = new ConcurrentHashMap<>();
  }

  /**
   * Creates a new {@link FnApiControlClientPoolService} which will enqueue and vend new SDK harness
   * connections.
   */
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
    FnApiControlClient newClient = FnApiControlClient.forRequestObserver(requestObserver);
    clients.put(newClient.asResponseObserver(), newClient);
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
    for (FnApiControlClient client : ImmutableList.copyOf(clients.values())) {
      try {
        // The close method of FnApiControlClient is idempotent
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Exception while closing client {}", client, e);
      }
    }
  }
}
