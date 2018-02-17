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

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.util.Map;

/**
 * A primitive implementation of {@link org.apache.beam.sdk.io.Read#from(BoundedSource)}.
 *
 * <p>Required as an intermediate transform until all runners support {@link
 * org.apache.beam.sdk.transforms.Impulse} as the only root primitive.
 */
public class PrimitiveBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {
  private final BoundedSource<T> source;

  public PrimitiveBoundedRead(BoundedSource<T> source) {
    this.source = source;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        WindowingStrategy.globalDefault(),
        PCollection.IsBounded.BOUNDED,
        source.getOutputCoder());
  }

  public static class Factory<T> implements PTransformOverrideFactory<PBegin, PCollection<T>, Read.Bounded<T>> {
    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Read.Bounded<T>> transform) {
      return PTransformReplacement.of(
          PBegin.in(transform.getPipeline()),
          new PrimitiveBoundedRead<T>(transform.getTransform().getSource()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
