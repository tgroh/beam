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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;

/**
 * TODO: Document
 */
public class QueryablePipeline {
  public static QueryablePipeline from(RunnerApi.Pipeline p) {
    return new QueryablePipeline(p);
  }

  private final Pipeline p;
  private final Map<String, String> pcollectionToProducer = new HashMap<>();
  private final SetMultimap<String, String> pcollectionToConsumers = HashMultimap.create();
  // TODO: private final SetMultimap<String, String> pcollectionToPerElementConsumers;

  private QueryablePipeline(Pipeline p) {
    this.p = p;
    for (Map.Entry<String, PTransform> transformEntry : p.getComponents().getTransformsMap().entrySet()) {
      String transformId = transformEntry.getKey();
      PTransform transform = transformEntry.getValue();
      if (transform.getSubtransformsCount() == 0) {
        for (String outputPc : transform.getOutputsMap().values()) {
          if (!transform.getInputsMap().values().contains(outputPc)) {
            String previous = pcollectionToProducer.put(outputPc, transformId);
            checkState(
                previous == null || previous.equals(transformId),
                "Multiple apparent produces for %s %s: %s and %s",
                PCollection.class.getSimpleName(),
                outputPc,
                previous,
                transformId);
          }
        }
        for (String inputPc : transform.getInputsMap().values()) {
          pcollectionToConsumers.put(inputPc, transformId);
          // TODO: if (perElementConsumer(inputPc, transform)) {
          // pcollectionToPerElementConsumers.put(inputPc, transformId);
          // }
        }
      }
    }
  }

  public String getProducer(String pcollectionId) {
    return pcollectionToProducer.get(pcollectionId);
  }

  public Iterable<String> getConsumers(String pcollectionId) {
    return pcollectionToConsumers.get(pcollectionId);
  }
}
