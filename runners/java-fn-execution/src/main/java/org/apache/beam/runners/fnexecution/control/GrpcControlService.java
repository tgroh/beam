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

import io.grpc.stub.StreamObserver;
import java.util.concurrent.SynchronousQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;

/** Created by tgroh on 11/3/17. */
public class GrpcControlService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements BeamFnControlService {
  // Clients that do not have any owner
  private final SynchronousQueue<FnControlClientHandler> newClients = new SynchronousQueue<>();

  @Override
  public StreamObserver<BeamFnApi.InstructionResponse> control(
      StreamObserver<BeamFnApi.InstructionRequest> outbound) {
    FnControlClientHandler handler = FnControlClientHandler.forOutboundObserver(outbound);
    try {
      newClients.put(handler);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return handler.responseObserver();
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public FnControlClientHandler getClient() {
    try {
      return newClients.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
