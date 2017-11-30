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

import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.data.FnDataReceiver;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.runners.local.TransformExecutor;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A {@link TransformExecutor} that executes a {@link PipelineStage} on a remote container via the
 * Beam Fn API.
 */
class RemoteStageExecutor {
  private final PipelineStage stage;
  private final Bundle<ByteString> bundle;
  private final SdkHarnessClient client;

  private final FnDataReceiver<ByteString> inputReceiver;
  private final ActiveBundle<ByteString> remoteBundle;

  private RemoteStageExecutor(
      PipelineStage stage, String processBundleDescriptorId, SdkHarnessClient client, Bundle<ByteString> bundle) {
    this.stage = stage;
    this.client = client;
    this.bundle = bundle;
    this.remoteBundle = client.newBundle(stage.getId());
    this.inputReceiver = remoteBundle.getInputReceiver();
  }

  public void run() {
    // TODO: Genericize
    // TODO: This may already be done;
    client.register(Collections.singleton(stage.toProcessBundleDescriptor()));
    ActiveBundle<?> activeBundle = client.newBundle(stage.getId());
    // TODO: Register outputs, roughly as:
    // for (OutputT outputTarget : stage.getOutputs()) {
    //     LogicalEndpoint outputEndpoint = LogicalEndpoint.of(activeBundle.getBundleId(),
    // Target.newBuilder().setName(outputTarget.getId())).build();
    //   active
    // }
    activeBundle.setOutputReceiver()
    FnDataReceiver inputReceiver = activeBundle.getInputReceiver();
    for (WindowedValue<ByteString> input : bundle) {
      try {
        inputReceiver.accept(input);
      } catch (Exception e) {
        throw new RuntimeException("Outward data transmission failed", e);
      }
    }
    try {
      ProcessBundleResponse response = activeBundle.getBundleResponse().get();
      if (response.hasMetrics()) {
        response.getMetrics();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          String.format("Interrupted while getting response for %s", activeBundle), e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    throw new UnsupportedOperationException("Unimplemented");
  }
}
