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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.MutationDetector;
import com.google.cloud.dataflow.sdk.util.MutationDetectors;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import com.google.common.base.Throwables;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * {@link ModelEnforcement} that enforces elements are not modified over the course of processing
 * an element.
 *
 * <p>Implies {@link EncodabilityEnforcment}.
 */
class ImmutabilityCheckingEnforcment<T> implements ModelEnforcement<T> {
  private final Map<CommittedBundle<T>, Map<T, MutationDetector>> mutationElements;
  private Coder<T> coder;

  public ImmutabilityCheckingEnforcment(
      AppliedPTransform<? extends TypedPValue<T>, ?, ?> transform) {
    mutationElements = new IdentityHashMap<>();
  }

  @Override
  public CommittedBundle<T> beforeStart(CommittedBundle<T> input) {
    coder = input.getPCollection().getCoder();
    Map<T, MutationDetector> elems = new IdentityHashMap<>();
    mutationElements.put(input, elems);
    for (WindowedValue<T> windowedValue : input.getElements()) {
      T value = windowedValue.getValue();
      try {
        elems.put(value, MutationDetectors.forValueWithCoder(value, coder));
      } catch (CoderException e) {
        Throwables.propagate(e);
      }
    }
    return input;
  }

  @Override
  public WindowedValue<T> beforeElement(WindowedValue<T> element) {
    return element;
  }

  @Override
  public void afterElement(WindowedValue<T> element) {}

  @Override
  public void afterFinish(
      CommittedBundle<T> input,
      InProcessTransformResult result,
      Iterable<? extends CommittedBundle<?>> committedOutputs) {
    for (MutationDetector detector : mutationElements.get(input).values()) {
      detector.verifyUnmodified();
    }
  }
}
