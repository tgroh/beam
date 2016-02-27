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
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Throwables;

/**
 * Enforces that all elements in a {@link PCollection} can be encoded using that
 * {@link PCollection PCollection's} {@link Coder}.
 */
class EncodabilityEnforcmentFactory<T> implements ModelEnforcementFactory<T> {

  @Override
  public <T> EncodabilityEnforcement<T> forBundle(CommittedBundle<T> input) {
    return new EncodabilityEnforcement<>(input);
  }

  public static class EncodabilityEnforcement<T> extends AbstractModelEnforcement<T> {
    private Coder<T> coder;
    public EncodabilityEnforcement(CommittedBundle<T> input) {
    coder = input.getPCollection().getCoder();
  }

  @Override
  public WindowedValue<T> beforeElement(WindowedValue<T> element) {
    try {
      return element.withValue(CoderUtils.clone(coder, element.getValue()));
    } catch (CoderException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void afterFinish(
      CommittedBundle<T> input,
      InProcessTransformResult result,
      Iterable<? extends CommittedBundle<?>> committedOutputs) {
    for (CommittedBundle<?> bundle : committedOutputs) {
      ensureBundleEncodable(bundle);
    }
  }

  private <T> void ensureBundleEncodable(CommittedBundle<T> bundle) {
    Coder<T> coder = bundle.getPCollection().getCoder();
    for (WindowedValue<T> value : bundle.getElements()) {
      try {
        CoderUtils.clone(coder, value.getValue());
      } catch (CoderException e) {
        Throwables.propagate(e);
      }
    }
    }
  }
}
