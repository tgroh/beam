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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsistentWithEqualsGroupingCoderEnforcementFactory
    implements ModelEnforcementFactory {

  private static class ConsistentWithEqualsModelEnforcement<T> extends AbstractModelEnforcement<T> {
    private final Map<Object, Collection<Object>> equalsGroupings = new HashMap<>();

    @Override
    public void beforeElement(WindowedValue<T> element) {
      checkArgument(
          element.getValue() instanceof KV,
          "%s expected to be applied only to elements of type %s, got %s",
          ConsistentWithEqualsModelEnforcement.class.getSimpleName(),
          KV.class.getSimpleName(),
          element.getValue().getClass().getSimpleName());
      KV<?, ?> elemKv = (KV<?, ?>) element.getValue();
      Collection<Object> vees = equalsGroupings.get(elemKv.getKey());
      if (vees == null) {
        vees = new ArrayList<>();
        equalsGroupings.put(elemKv.getKey(), vees);
      }
      vees.add(elemKv.getValue());
    }

    @Override
    public void afterFinish(
        CommittedBundle<T> input,
        InProcessTransformResult result,
        Iterable<? extends CommittedBundle<?>> outputs) {
      for (CommittedBundle<?> output : outputs) {
        Object key = output.getKey();
        Collection<Object> expectedVees = equalsGroupings.get(key);
        checkNotNull(
            expectedVees,
            "Got output with key %s but no such key exists in expected groupings",
            key);
        Iterable<?> outputElements = output.getElements();
        Iterable<?> actualVees =
            ((KV<?, Iterable<?>>) Iterables.getOnlyElement(outputElements)).getValue();
        Iterables.removeAll(expectedVees, actualVees);
      }
    }
  }

  @Override
  public <T> ModelEnforcement<T> forBundle(
      CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
    if (input.getPCollection().getCoder().consistentWithEquals()) {
      return new ConsistentWithEqualsModelEnforcement<>();
    }
    return new EmptyModelEnforcement<>();
  }
}
