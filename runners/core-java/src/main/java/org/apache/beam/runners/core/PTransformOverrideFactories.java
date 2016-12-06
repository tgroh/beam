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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * Utility methods for implementors of {@link PTransformOverrideFactory}.
 */
public final class PTransformOverrideFactories {
  private PTransformOverrideFactories() {}
  public static <InputT extends PValue> InputT getOnlyInput(List<TaggedPValue> expansion) {
    checkArgument(
        expansion.size() == 1,
        "Expected expansion to have exactly one component %s, got %s",
        TaggedPValue.class.getSimpleName(),
        expansion.size());
    return (InputT) expansion.get(0).getValue();
  }
}
