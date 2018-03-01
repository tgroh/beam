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

package org.apache.beam.runners.direct;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.SimpleProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.RemoteOutputReceiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that executes an {@link ExecutableStage} on a remote SDK
 * Harness via the Beam Portability Framework.
 */
class RemoteStageExecutorFactory implements TransformEvaluatorFactory {
  private final SdkHarnessClient controlClient;

  RemoteStageExecutorFactory(EvaluationContext ctxt) {
    // TODO: actually instantiate with real services
    controlClient = SdkHarnessClient.usingFnApiClient(null, null);
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    return null;
  }

  @Override
  public void cleanup() throws Exception {
    controlClient.close();
  }

  private static class RemoteStageEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final AppliedPTransform<?, ?, ?> transform;

    private final ActiveBundle<InputT> bundle;
    private final Map<PCollection<?>, UncommittedBundle<?>> pcollectionBundles = new HashMap<>();

    private RemoteStageEvaluator(
        AppliedPTransform<?, ?, ?> transform,
        EvaluationContext ctxt,
        SdkHarnessClient client,
        SimpleProcessBundleDescriptor desc) {
      this.transform = transform;
      // TODO: Side inputs, when we support side inputs in the ReferenceRunner
      Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Map.Entry<BeamFnApi.Target, Coder<WindowedValue<?>>> outputCoder :
          desc.getOutputTargetCoders().entrySet()) {
        // TODO: Get from the context, or the graph
        PCollection<?> output = PCollection.createPrimitiveOutputInternal(null, null, null, null);
        UncommittedBundle<?> bundle =
            pcollectionBundles.computeIfAbsent(output, ctxt::createBundle);
        RemoteOutputReceiver<?> receiver =
            RemoteOutputReceiver.of(
                outputCoder.getValue(), elem -> bundle.add((WindowedValue) elem));
        outputReceivers.put(outputCoder.getKey(), receiver);
      }
      this.bundle =
          client
              .getProcessor(
                  desc.getProcessBundleDescriptor(),
                  (RemoteInputDestination<WindowedValue<InputT>>)
                      (RemoteInputDestination) desc.getRemoteInputDestination())
              .newBundle(outputReceivers);
    }

    @Override
    public void processElement(WindowedValue<InputT> element) throws Exception {
      bundle.getInputReceiver().accept(element);
    }

    @Override
    public TransformResult<InputT> finishBundle() throws Exception {
      // TODO: State updates, when we support user state in portability
      // TODO: Timer updates, when we support user timers in portability
      for (InboundDataClient inboundDataClient : bundle.getOutputClients().values()) {
        inboundDataClient.awaitCompletion();
      }
      return StepTransformResult.<InputT>withoutHold(transform)
          .addOutput(pcollectionBundles.values())
          .build();
    }
  }
}
