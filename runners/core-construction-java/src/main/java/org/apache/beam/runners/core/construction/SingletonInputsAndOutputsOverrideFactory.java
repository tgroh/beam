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

package org.apache.beam.runners.core.construction;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} where the input and output types are {@link PValue PValues},
 * with implementations for reconstructing input and mapping outputs.
 */
public abstract class SingletonInputsAndOutputsOverrideFactory<
        InputT extends PValue,
        OutputT extends PValue,
        TransformT extends PTransform<InputT, OutputT>>
    implements PTransformOverrideFactory<InputT, OutputT, TransformT> {
  /**
   * {@inheritDoc}
   *
   * @return the only input value.
   */
  @Override
  public final InputT getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    return (InputT) Iterables.getOnlyElement(inputs);
  }

  /**
   * {@inheritDoc}.
   *
   * @return a singleton map of the replacement value to the original value.
   */
  @Override
  public final Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, OutputT newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
