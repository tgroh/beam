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
package org.apache.beam.runners.dataflow.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.Coders;
import org.apache.beam.runners.core.construction.ParDos;
import org.apache.beam.runners.core.construction.WindowingStrategies;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Wrapper class holding the necessary information to serialize a {@link DoFn}.
 *
 * @param <InputT> the type of the (main) input elements of the {@link DoFn}
 * @param <OutputT> the type of the (main) output elements of the {@link DoFn}
 */
public class DoFnInfo<InputT, OutputT> implements Serializable {
  private final DoFn<InputT, OutputT> doFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;
  private final TupleTag<?> mainOutputTag;
  private final Map<Long, TupleTag<?>> outputMap;

  /**
   * Creates a {@link DoFnInfo} for the given {@link DoFn}.
   */
  public static <InputT, OutputT> DoFnInfo<InputT, OutputT> forFn(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> inputWindowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      TupleTag<?> mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    return new DoFnInfo<>(
        doFn, inputWindowingStrategy, sideInputViews, inputCoder, mainOutput, outputMap);
  }

  public static <InputT, OutputT> DoFnInfo<InputT, OutputT> forRunnerApiPTransform(
      RunnerApi.PTransform pt, RunnerApi.Components components)
      throws IOException {
    ParDoPayload payload = pt.getSpec().getParameter().unpack(ParDoPayload.class);
    DoFn<InputT, OutputT> doFn = (DoFn<InputT, OutputT>) ParDos.getDoFn(payload);
    RunnerApi.PCollection inputPCollection = ParDos.getMainInput(pt, components);
    WindowingStrategy<?, ?> inputWindowingStrategy =
        WindowingStrategies.fromProto(
            components.getWindowingStrategiesOrThrow(inputPCollection.getWindowingStrategyId()),
            components);
    Coder<InputT> inputCoder =
        (Coder<InputT>)
            Coders.fromProto(
                components.getCodersOrThrow(inputPCollection.getCoderId()), components);

    List<PCollectionView<?>> sideInputViews = new ArrayList<>();
    for (Map.Entry<String, SideInput> sideInput : payload.getSideInputsMap().entrySet()) {
      sideInputViews.add(
          ParDos.fromProto(sideInput.getValue(), sideInput.getKey(), pt, components));
    }

    Map<Long, TupleTag<?>> outputMap = new HashMap<>();

    return forFn(
        doFn,
        inputWindowingStrategy,
        sideInputViews,
        inputCoder,
        ParDos.getMainOutputTag(payload),
        outputMap);
  }

  public DoFnInfo<InputT, OutputT> withFn(DoFn<InputT, OutputT> newFn) {
    return DoFnInfo.forFn(
        newFn, windowingStrategy, sideInputViews, inputCoder, mainOutputTag, outputMap);
  }

  private DoFnInfo(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      TupleTag<?> mainOutputTag,
      Map<Long, TupleTag<?>> outputMap) {
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;
    this.mainOutputTag = mainOutputTag;
    this.outputMap = outputMap;
  }

  /** Returns the embedded function. */
  public DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public Iterable<PCollectionView<?>> getSideInputViews() {
    return sideInputViews;
  }

  public Coder<InputT> getInputCoder() {
    return inputCoder;
  }

  public TupleTag<?> getMainOutput() {
    return mainOutputTag;
  }

  public Map<Long, TupleTag<?>> getOutputMap() {
    return outputMap;
  }
}
