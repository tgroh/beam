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
 *
 */

package org.apache.beam.runners.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that consumes from a single {@link PValue} and produces a
 * single {@link PValue}.
 */
public abstract class SimplePTransformOverrideFactory<
        InputT extends PValue,
        OutputT extends PValue,
        TransformT extends PTransform<InputT, OutputT>>
    implements PTransformOverrideFactory<InputT, OutputT, TransformT> {
  @Override
  public final InputT createInputFromExpansion(
      Pipeline p, List<TaggedPValue> expansion) {
    return PTransformOverrideFactories.getOnlyInput(expansion);
  }

  @Override
  public final Map<PValue, PValue> getOriginalToReplacements(
      InputT input, TransformT originalTransform, OutputT originalOutput, OutputT replacedOutput) {
    return Collections.<PValue, PValue>singletonMap(originalOutput, replacedOutput);
  }
}
