/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} which wraps the result of another
 * {@link TransformEvaluatorFactory} within a {@link TransformEvaluator} that applies
 * {@link ModelEnforcement}.
 */
public class ModelEnforcingTransformEvaluatorFactory implements TransformEvaluatorFactory {
  private final TransformEvaluatorFactory wrapped;
  private final List<ModelEnforcementFactory> enforcements;

  public ModelEnforcingTransformEvaluatorFactory(
      TransformEvaluatorFactory wrapped, List<ModelEnforcementFactory> enforcements) {
    this.wrapped = wrapped;
    this.enforcements = enforcements;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws Exception {
    @SuppressWarnings({"rawtypes", "unchecked"})
    List<ModelEnforcement<InputT>> bundleEnforcements =
        createEnforcements(application, (CommittedBundle) inputBundle);
    TransformEvaluator<InputT> underlyingEvaluator =
        wrapped.forApplication(application, inputBundle, evaluationContext);
    if (!bundleEnforcements.isEmpty()) {
      return underlyingEvaluator;
    } else {
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformEvaluator<InputT> evaluator =
          createEnforcingEvaluator(
              (CommittedBundle) inputBundle, bundleEnforcements, underlyingEvaluator);
      return evaluator;
    }
  }

  private <InputT> TransformEvaluator<InputT> createEnforcingEvaluator(
      CommittedBundle<InputT> inputBundle,
      List<ModelEnforcement<InputT>> bundleEnforcements,
      TransformEvaluator<InputT> underlyingEvaluator)
      throws Exception {
    return new ModelEnforcingTransformEvaluator<InputT>(
        inputBundle, bundleEnforcements, underlyingEvaluator);
  }

  private <InputT> List<ModelEnforcement<InputT>> createEnforcements(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<InputT> inputBundle) {
    if (inputBundle != null) {
      List<ModelEnforcement<InputT>> bundleEnforcements = new ArrayList<>();
      for (ModelEnforcementFactory enforcementFactory : enforcements) {
        bundleEnforcements.add(enforcementFactory.forBundle(inputBundle, application));
      }
      return bundleEnforcements;
    } else {
      return Collections.<ModelEnforcement<InputT>>emptyList();
    }
  }

  private static class ModelEnforcingTransformEvaluator<T> implements TransformEvaluator<T> {
    private final CommittedBundle<T> input;
    private final List<ModelEnforcement<T>> enforced;
    private final TransformEvaluator<T> wrapped;

    public ModelEnforcingTransformEvaluator(
        CommittedBundle<T> input,
        List<ModelEnforcement<T>> enforced,
        TransformEvaluator<T> wrapped) {
      this.input = input;
      this.enforced = enforced;
      this.wrapped = wrapped;
    }

    @Override
    public void processElement(WindowedValue<T> element) throws Exception {
      WindowedValue<T> enforcedElement = element;
      for (ModelEnforcement<T> enforce : enforced) {
        enforcedElement = enforce.beforeElement(enforcedElement);
      }
      wrapped.processElement(enforcedElement);
      for (ModelEnforcement<T> enforce : enforced) {
        enforce.afterElement(enforcedElement);
      }
    }

    @Override
    public InProcessTransformResult finishBundle() throws Exception {
      InProcessTransformResult result = wrapped.finishBundle();
      for (ModelEnforcement<T> enforce : enforced) {
        enforce.afterFinish(input, result);
      }
      return result;
    }

  }
}
