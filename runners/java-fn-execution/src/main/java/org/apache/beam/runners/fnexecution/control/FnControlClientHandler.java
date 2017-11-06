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

import com.google.auto.value.AutoValue;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest;
import org.apache.beam.runners.fnexecution.SynchronizedStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An adapter which consumes {@link BeamFnApi.InstructionRequest Instruction Requests} and forwards
 * them to a client. When the client responds that it has completed the request, any supplied
 * callback will be invoked with the response.
 *
 * <p>If the client hangs up or has errored, then all pending requests are completed exceptionally.
 *
 * <p>If the server hangs up or has errored, then all pending requests are completed exceptionally.
 *
 * <p>A {@link FnControlClientHandler} can be invoked used by multiple threads.
 */
public class FnControlClientHandler implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FnControlClientHandler.class);

  /**
   * Create a new {@link FnControlClientHandler} which will send messages to an SDK harness using
   * the provided {@link StreamObserver}.
   */
  static FnControlClientHandler forOutboundObserver(
      StreamObserver<BeamFnApi.InstructionRequest> outboundObserver) {
    return new FnControlClientHandler(outboundObserver);
  }

  /**
   * The supplier for Instruction IDs oif Instruction Requests built by this {@link
   * FnControlClientHandler}
   */
  private final Supplier<String> instructionIdSupplier =
      new Supplier<String>() {
        final AtomicLong id = new AtomicLong(-1);

        @Override
        public String get() {
          return Long.toString(id.getAndDecrement());
        }
      };

  private final SynchronizedStreamObserver<InstructionRequest> outboundRequestObserver;
  private final ConcurrentMap<String, InstructionHandle> activeInstructions = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final ClientResponseObserver responseObserver = new ClientResponseObserver();

  private FnControlClientHandler(
      StreamObserver<BeamFnApi.InstructionRequest> outboundRequestObserver) {
    this.outboundRequestObserver = SynchronizedStreamObserver.wrapping(outboundRequestObserver);
  }

  public void register(RegisterRequest registerRequest) {
    String id = instructionIdSupplier.get();
    activeInstructions.put(id, null);
    outboundRequestObserver.onNext(
        InstructionRequest.newBuilder()
            .setInstructionId(id)
            .setRegister(registerRequest)
            .build());
  }

  public ActiveBundle processBundle(ProcessBundleRequest processBundleRequest) {
    String id = instructionIdSupplier.get();
    outboundRequestObserver.onNext(
        InstructionRequest.newBuilder()
            .setInstructionId(id)
            .setProcessBundle(processBundleRequest)
            .build());
    return new ActiveBundle(id, this);
  }

  @Override
  public void close() throws Exception {
    cleanupAndClose(new IllegalStateException("Server hanged up"));
  }

  private void cleanupAndClose(Throwable t) {
    if (!closed.getAndSet(true)) {
      // This call is the first to reach this method, and is responsible for cleaning up.
      ImmutableMap<String, InstructionHandle> activeInstructionsCopy =
          ImmutableMap.copyOf(activeInstructions);
      activeInstructions.clear();

      if (activeInstructionsCopy.isEmpty()) {
        outboundRequestObserver.onCompleted();
      } else {
        for (ImmutableMap.Entry<String, InstructionHandle> activeInstruction :
            activeInstructionsCopy.entrySet()) {
          activeInstruction.getValue().completeExceptionally(t);
        }

        outboundRequestObserver.onError(
            Status.CANCELLED.withDescription(t.getMessage()).asRuntimeException());
      }
    }
  }

  /**
   * Returns a {@link StreamObserver} which coordinates between outbound requests sent by this
   * {@link FnControlClientHandler} and the responses to those requests sent by an SDK harness.
   */
  StreamObserver<InstructionResponse> responseObserver() {
    return responseObserver;
  }

  private class ClientResponseObserver implements StreamObserver<BeamFnApi.InstructionResponse> {
    @Override
    public void onNext(InstructionResponse value) {
      InstructionHandle completed = activeInstructions.remove(value.getInstructionId());
      completed.complete();
    }

    @Override
    public void onError(Throwable t) {
      cleanupAndClose(t);
    }

    @Override
    public void onCompleted() {
      cleanupAndClose(new IllegalStateException("Client hanged up"));
    }
  }

  @AutoValue
  abstract static class InstructionHandle {
    abstract InstructionRequest.RequestCase getRequestCase();
    @Nullable abstract ActiveBundle getBundle();

    void complete() {}
    void completeExceptionally(Throwable t) {}
  }
}
