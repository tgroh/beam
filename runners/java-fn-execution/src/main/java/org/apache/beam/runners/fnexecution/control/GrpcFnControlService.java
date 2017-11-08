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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by tgroh on 11/9/17.
 */
public class GrpcFnControlService implements AutoCloseable {

  private final FnApiControlClientPoolService clientPoolService;

  public static GrpcFnControlService withSynchronousPool() {
    return new GrpcFnControlService(new SynchronousQueue<FnApiControlClient>());
  }

  private final BlockingQueue<FnApiControlClient> clientPool;

  private GrpcFnControlService(BlockingQueue<FnApiControlClient> clientPool) {
    this.clientPool = clientPool;
    clientPoolService = FnApiControlClientPoolService.offeringClientsToPool(clientPool);
  }

  /**
   * Obtains an {@link SdkHarnessClient} for a client connected to this {@link
   * GrpcFnControlService}, blocking until one is available.
   */
  public SdkHarnessClient takeClient() throws InterruptedException {
    FnApiControlClient controlClient = clientPool.take();
    return SdkHarnessClient.usingFnApiClient(controlClient);
  }

  @Override
  public void close() throws Exception {
  }
}
