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
import java.util.concurrent.ExecutionException;
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
class RemoteStageExecutor implements TransformExecutor {
  private final String processBundleDescriptorId;
  private final Bundle<ByteString> bundle;
  private final SdkHarnessClient client;

  private RemoteStageExecutor(
      String processBundleDescriptorId, SdkHarnessClient client, Bundle<ByteString> bundle) {
    this.processBundleDescriptorId = processBundleDescriptorId;
    this.client = client;
    this.bundle = bundle;
  }

  @Override
  public void run() {
    // TODO: Genericize
    ActiveBundle activeBundle = client.newBundle(processBundleDescriptorId);
    FnDataReceiver inputReceiver = activeBundle.getInputReceiver();
    for (WindowedValue<ByteString> input : bundle) {
      try {
        inputReceiver.accept(input);
      } catch (Exception e) {
        throw new RuntimeException("Outward data transmission failed", e);
      }
    }
    try {
      activeBundle.getBundleResponse().get();
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
