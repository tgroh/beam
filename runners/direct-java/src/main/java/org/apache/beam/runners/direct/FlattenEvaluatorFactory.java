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

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.FlattenPCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import javax.annotation.Nullable;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the {@link Flatten}
 * {@link PTransform}.
 */
class FlattenEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <InputT> TransformEvaluator<InputT> create(
      AppliedPTransform<?, ?, ?> application,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = (TransformEvaluator<InputT>) createInMemoryEvaluator(
            (AppliedPTransform) application, evaluationContext);
    return evaluator;
  }

  private <InputT> TransformEvaluator<InputT> createInMemoryEvaluator(
      final AppliedPTransform<
              PCollectionList<InputT>, PCollection<InputT>, FlattenPCollectionList<InputT>>
          application,
      final InProcessEvaluationContext evaluationContext) {
    return new FlattenEvaluator<>(application, evaluationContext);
  }

  private static class FlattenEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final AppliedPTransform<
        PCollectionList<InputT>, PCollection<InputT>, FlattenPCollectionList<InputT>>
        application;
    private final InProcessEvaluationContext evaluationContext;
    private UncommittedBundle<InputT> outputBundle;
    private InProcessTransformResult result;

    public FlattenEvaluator(
        AppliedPTransform<
            PCollectionList<InputT>, PCollection<InputT>, FlattenPCollectionList<InputT>>
            application,
        InProcessEvaluationContext evaluationContext) {
      this.application = application;
      this.evaluationContext = evaluationContext;
    }

    @Override
    public void startBundle(@Nullable CommittedBundle<InputT> inputBundle) {
      Builder transformResultBuilder = StepTransformResult.withoutHold(application);
      if (inputBundle != null) {
        outputBundle = evaluationContext.createBundle(inputBundle, application.getOutput());
        transformResultBuilder = transformResultBuilder.addOutput(outputBundle);
      }
      result = transformResultBuilder.build();
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      outputBundle.add(element);
    }

    @Override
    public InProcessTransformResult finishBundle() {
      return result;
    }
  }
}
