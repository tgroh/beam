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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utility methods for creating {@link ReplacementOutput} for known styles of {@link POutput}.
 */
public class ReplacementOutputs {
  private ReplacementOutputs() {}

  public static Map<PCollection<?>, ReplacementOutput> singleton(
      Map<TupleTag<?>, PValue> original, PCollection<?> replacement) {
    Entry<TupleTag<?>, PValue> originalElement = Iterables.getOnlyElement(original.entrySet());
    TupleTag<?> replacementTag = Iterables.getOnlyElement(replacement.expand().entrySet()).getKey();
    return Collections.<PCollection<?>, ReplacementOutput>singletonMap(
        replacement,
        ReplacementOutput.of(
            TaggedPCollection.of(
                originalElement.getKey(), (PCollection<?>) originalElement.getValue()),
            TaggedPCollection.of(replacementTag, replacement)));
  }

  public static Map<PCollection<?>, ReplacementOutput> tuple(
      Map<TupleTag<?>, PValue> original, PCollectionTuple replacement) {
    Map<TupleTag<?>, TaggedPCollection> originalTags = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PValue> originalValue : original.entrySet()) {
      checkArgument(
          originalValue.getValue() instanceof PCollection,
          "Can only replace %s outputs",
          PCollection.class.getSimpleName());
      originalTags.put(
          originalValue.getKey(),
          TaggedPCollection.of(originalValue.getKey(), (PCollection<?>) originalValue.getValue()));
    }
    ImmutableMap.Builder<PCollection<?>, ReplacementOutput> resultBuilder = ImmutableMap.builder();
    Set<TupleTag<?>> missingTags = new HashSet<>(originalTags.keySet());
    for (Map.Entry<TupleTag<?>, PCollection<?>> replacementValue :
        replacement.getAll().entrySet()) {
      TaggedPCollection mapped = originalTags.get(replacementValue.getKey());
      checkArgument(
          mapped != null,
          "Missing original output for Tag %s and Value %s Between original %s and replacement %s",
          replacementValue.getKey(),
          replacementValue.getValue(),
          original,
          replacement.expand());
      resultBuilder.put(
          replacementValue.getValue(),
          ReplacementOutput.of(
              mapped,
              TaggedPCollection.of(replacementValue.getKey(), replacementValue.getValue())));
      missingTags.remove(replacementValue.getKey());
    }
    ImmutableMap<PCollection<?>, ReplacementOutput> result = resultBuilder.build();
    checkArgument(
        missingTags.isEmpty(),
        "Missing replacement for tags %s. Encountered tags: %s",
        missingTags,
        result.keySet());
    return result;
  }
}
