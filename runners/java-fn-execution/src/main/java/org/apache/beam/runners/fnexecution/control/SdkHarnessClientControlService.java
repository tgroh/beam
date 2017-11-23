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

import io.grpc.ServerServiceDefinition;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.FnService;

/**
 * A {@link FnService} which implements the {@link BeamFnControlGrpc Beam Fn Control Service}.
 *
 * <p>Uses a {@link FnApiControlClientPoolService} as the underlying implementation.
 */
public class SdkHarnessClientControlService implements FnService {
  /**
   * Create a new {@link SdkHarnessClientControlService} which will use a {@link SynchronousQueue}
   * as the underlying client pool. This will cause calls to {@link #getClient()} to block until a
   * client connects.
   */
  public static SdkHarnessClientControlService synchronous() {
    SynchronousQueue<FnApiControlClient> pool = new SynchronousQueue<>();
    return new SdkHarnessClientControlService(
        pool, FnApiControlClientPoolService.offeringClientsToPool(pool));
  }

  private final BlockingQueue<FnApiControlClient> pool;
  private final FnApiControlClientPoolService poolService;

  private SdkHarnessClientControlService(
      BlockingQueue<FnApiControlClient> pool, FnApiControlClientPoolService poolService) {
    this.pool = pool;
    this.poolService = poolService;
  }

  public SdkHarnessClient getClient() {
    try {
      return SdkHarnessClient.usingFnApiClient(pool.take());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    poolService.close();
  }

  @Override
  public ServerServiceDefinition bindService() {
    return poolService.bindService();
  }
}
