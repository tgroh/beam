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

package org.apache.beam.runners.direct.portable;

import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@link TransformEvaluatorFactory} which produces {@link TransformEvaluator evaluators} for
 * stages which execute on an SDK harness via the Fn Execution APIs.
 */
class RemoteStageEvaluatorFactory implements TransformEvaluatorFactory {

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    ExecutableStage stage =
        ExecutableStage.fromPayload(
            ExecutableStagePayload.parseFrom(application.getTransform().getSpec().getPayload()));
    return null;
  }

  @Override
  public void cleanup() throws Exception {
    // envManager.close();
  }

  private class RemoteStageEvaluator<T> implements TransformEvaluator<T> {
    private final PTransformNode transform;
    private final ActiveBundle<T> bundle;
    private final Collection<UncommittedBundle<?>> outputs;

    private RemoteStageEvaluator(PTransformNode transform) {
      // TODO: Retrieve from the appropriate abstraction.
      this.transform = transform;
      outputs = new ArrayList<>();
      bundle = null;
    }

    @Override
    public void processElement(WindowedValue<T> element) throws Exception {
      bundle.getInputReceiver().accept(element);
    }

    @Override
    public TransformResult<T> finishBundle() throws Exception {
      bundle.close();
      return StepTransformResult.<T>withoutHold(transform).addOutput(outputs).build();
    }
  }
}
