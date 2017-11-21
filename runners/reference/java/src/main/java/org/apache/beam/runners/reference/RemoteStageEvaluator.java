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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.sdk.util.WindowedValue;

/** TODO: Document */
public class RemoteStageEvaluator<T> {
  public static <T> RemoteStageEvaluator<T> create(
      PipelineStage stage, SdkHarnessClient client, Bundle<T> elems) {}

  private final PipelineStage stage;
  private final SdkHarnessClient client;

  private ActiveBundle<WindowedValue<T>> bundle;

  private RemoteStageEvaluator(
      PipelineStage stage, SdkHarnessClient client) {
    this.stage = stage;
    this.client = client;
  }

  public void processElement(WindowedValue<T> element) throws Exception {
    bundle.getInputReceiver().accept(element);
  }

  public void finishBundle() throws Exception {
    bundle.getInputReceiver().close();
    // Block until the stage is complete
    ProcessBundleResponse response = bundle.getBundleResponse().get();
  }
}
