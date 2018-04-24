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

import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.ExecutableStageTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;

/** Execute an {@link ExecutableStage} via the Fn API. */
class RemoteStageEvaluatorFactory
    implements TransformEvaluatorFactory<AppliedPTransform<?, ?, ?>> {
  public RemoteStageEvaluatorFactory(EvaluationContext ctxt) {}

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> executable, CommittedBundle<?> inputBundle) throws Exception {
    ExecutableStagePayload payload =
        ExecutableStageTranslation.getExecutableStagePayload(executable);
    ExecutableStage stage = ExecutableStage.fromPayload(payload);
    
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanup() throws Exception {}

  private static class RemoteStageEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final ActiveBundle<InputT> remoteBundle;
    private final StepTransformResult.Builder<InputT> resultBuilder;

    private RemoteStageEvaluator(ActiveBundle<InputT> remoteBundle, Builder<InputT> resultBuilder) {
      this.remoteBundle = remoteBundle;
      this.resultBuilder = resultBuilder;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) throws Exception {
      remoteBundle.getInputReceiver().accept(element);
    }

    @Override
    public TransformResult<InputT> finishBundle() throws Exception {
      remoteBundle.close();
      return resultBuilder.build();
    }
  }
}
