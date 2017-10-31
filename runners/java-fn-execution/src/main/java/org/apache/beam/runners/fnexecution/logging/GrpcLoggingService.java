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

package org.apache.beam.runners.fnexecution.logging;

import com.google.common.collect.ImmutableSet;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the Beam Fn Logging Service over GRPC. */
public class GrpcLoggingService extends BeamFnLoggingGrpc.BeamFnLoggingImplBase
    implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcLoggingService.class);

  public static GrpcLoggingService create(LogWriter logWriter) {
    return new GrpcLoggingService(logWriter);
  }

  // TODO: Add a FlowControlStreamObserverFactory of some sort
  private final LogWriter logWriter;
  private final ConcurrentMap<InboundLogObserver, StreamObserver<?>> connectedClients;

  private GrpcLoggingService(LogWriter logWriter) {
    this.logWriter = logWriter;
    this.connectedClients = new ConcurrentHashMap<>();
  }

  @Override
  public StreamObserver<BeamFnApi.LogEntry.List> logging(
      StreamObserver<BeamFnApi.LogControl> outboundControlObserver) {
    return new InboundLogObserver();
  }

  @Override
  public void close() throws Exception {
    Set<InboundLogObserver> remainingClients = ImmutableSet.copyOf(connectedClients.keySet());
    if (!remainingClients.isEmpty()) {
      LOG.info(
          "{} Beam Fn Logging clients still connected during shutdown.", remainingClients.size());

      // Signal server shutting down to all remaining connected clients.
      for (InboundLogObserver client : remainingClients) {
        // We remove these from the connected clients map to prevent a race between
        // this close method and the InboundObserver calling a terminal method on the
        // StreamObserver. If we removed it, then we are responsible for the terminal call.
        completeIfNotNull(connectedClients.remove(client));
      }
    }
  }

  private void completeIfNotNull(@Nullable StreamObserver<?> remove) {
    if (remove != null) {
      try {
        remove.onCompleted();
      } catch (RuntimeException ignored) {
        LOG.warn("Beam Fn Logging client failed to be complete.", ignored);
      }
    }
  }

  private class InboundLogObserver implements StreamObserver<BeamFnApi.LogEntry.List> {
    @Override
    public void onNext(BeamFnApi.LogEntry.List value) {
      for (BeamFnApi.LogEntry logEntry : value.getLogEntriesList()) {
        logWriter.log(logEntry);
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Logging client failed unexpectedly.", t);
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));

    }

    @Override
    public void onCompleted() {
      LOG.info("Logging client hanged up.");
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));
    }
  }
}
