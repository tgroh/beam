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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/** Created by tgroh on 3/3/17. */
public abstract class SingleOutputRootOverrideFactory<
        T, TransformT extends PTransform<PBegin, PCollection<T>>>
    implements PTransformOverrideFactory<PBegin, PCollection<T>, TransformT> {
  public abstract PTransform<PBegin, PCollection<T>> getReplacementTransform(
      TransformT transform, PCollection<T> originalOutput);

  @Override
  public final PTransform<PBegin, PCollection<T>> getReplacementTransform(
      TransformT transform, List<TaggedPValue> originalOutputs) {
    PValue originalOutput = Iterables.getOnlyElement(originalOutputs).getValue();
    return getReplacementTransform(transform, (PCollection<T>) originalOutput);
  }

  @Override
  public final PBegin getInput(List<TaggedPValue> inputs, Pipeline p) {
    return p.begin();
  }

  @Override
  public final Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PCollection<T> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
