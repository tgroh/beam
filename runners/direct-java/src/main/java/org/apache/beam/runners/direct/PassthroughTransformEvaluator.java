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

import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;

class PassthroughTransformEvaluator<InputT> implements TransformEvaluator<InputT> {
  public static <InputT> PassthroughTransformEvaluator<InputT> create(
      InProcessEvaluationContext evaluationContext,
      AppliedPTransform<PCollection<InputT>, PCollection<InputT>, ?> transform) {
    return new PassthroughTransformEvaluator<>(evaluationContext, transform);
  }

  private final InProcessEvaluationContext evaluationContext;
  private final AppliedPTransform<PCollection<InputT>, PCollection<InputT>, ?> transform;

  private UncommittedBundle<InputT> output;

  private PassthroughTransformEvaluator(
      InProcessEvaluationContext evaluationContext,
      AppliedPTransform<PCollection<InputT>, PCollection<InputT>, ?> transform) {
    this.evaluationContext = evaluationContext;
    this.transform = transform;
  }

  @Override
  public void startBundle(@Nullable InProcessPipelineRunner.CommittedBundle<InputT> inputBundle) {
    this.output = evaluationContext.createBundle(inputBundle, transform.getOutput());
  }

  @Override
  public void processElement(WindowedValue<InputT> element) throws Exception {
    output.add(element);
  }

  @Override
  public InProcessTransformResult finishBundle() throws Exception {
    return StepTransformResult.withoutHold(transform).addOutput(output).build();
  }

}
