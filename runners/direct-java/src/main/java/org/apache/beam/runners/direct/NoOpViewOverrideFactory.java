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

import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Created by tgroh on 5/30/17. */
class NoOpViewOverrideFactory<ElemT, ViewT>
    implements PTransformOverrideFactory<
        PCollection<ElemT>, PCollectionView<ViewT>,
        PTransform<PCollection<ElemT>, PCollectionView<ViewT>>> {
  public static final String DIRECT_NO_OP_URN =
      "urn:beam:directrunner:transforms:no_op:v1";
  @Override
  public PTransformReplacement<PCollection<ElemT>, PCollectionView<ViewT>> getReplacementTransform(
      AppliedPTransform transform) {
    RawPTransform<PCollection<ElemT>, PCollectionView<ViewT>, Message> noOpTransform =
       new NoOpTransform<>();
    return PTransformReplacement.of(
        (PCollection<ElemT>) Iterables.getOnlyElement(transform.getInputs().values()),
        noOpTransform);
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollectionView<ViewT> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

  static class NoOpTransform<ElemT, ViewT>
      extends RawPTransform<PCollection<ElemT>, PCollectionView<ViewT>, Message> {
    @Override
    public PCollectionView<ViewT> expand(PCollection<ElemT> input) {
      // Doesn't actually work. This depends on the replacement always being discarded.
      return (PCollectionView)
          PCollectionViews.iterableView(input, input.getWindowingStrategy(), input.getCoder());
    }

    @Override
    public String getUrn() {
      return DIRECT_NO_OP_URN;
    }
  }
}
