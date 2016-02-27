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
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import java.util.List;

class ChainedModelEnforcment<T> implements ModelEnforcement<T> {
  private List<ModelEnforcement<T>> beneath;

  @Override
  public CommittedBundle<T> beforeStart(CommittedBundle<T> input) {
    CommittedBundle<T> resultBundle = input;
    for (ModelEnforcement<T> chainedEnforcment : beneath) {
      resultBundle = chainedEnforcment.beforeStart(resultBundle);
    }
    return resultBundle;
  }

  @Override
  public WindowedValue<T> beforeElement(WindowedValue<T> element) {
    WindowedValue<T> result = element;
    for (ModelEnforcement<T> chainedEnforcment : beneath) {
      result = chainedEnforcment.beforeElement(result);
    }
    return result;
  }

  @Override
  public void afterElement(WindowedValue<T> element) {
    for (ModelEnforcement<T> chainedEnforcment : beneath) {
      chainedEnforcment.afterElement(element);
    }
  }

  @Override
  public void afterFinish(
      CommittedBundle<T> input,
      InProcessTransformResult result,
      Iterable<? extends CommittedBundle<?>> committedOutputs) {
    for (ModelEnforcement<T> chainedEnforcment : beneath) {
      chainedEnforcment.afterFinish(input, result, committedOutputs);
    }
  }
}
