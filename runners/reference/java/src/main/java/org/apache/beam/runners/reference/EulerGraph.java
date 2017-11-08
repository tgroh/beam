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

package org.apache.beam.runners.reference;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;

/**
 * Created by tgroh on 11/9/17.
 */
public class EulerGraph {

  public static EulerGraph from(RunnerApi.Pipeline pipeline) {
    return new EulerGraph(pipeline);
  }

  private final Pipeline pipeline;
  private final Map<RunnerApi.PCollection, RunnerApi.PTransform> producers;
  // Order is unimportant; a PTransform appearing multiple times is required.
  private final ListMultimap<PCollection, PTransform> consumers;

  private EulerGraph(Pipeline pipeline) {
    this.pipeline = pipeline;
    ImmutableMap.Builder<RunnerApi.PCollection, RunnerApi.PTransform> producers =
        ImmutableMap.builder();
    ImmutableListMultimap.Builder<PCollection, PTransform> consumers =
        ImmutableListMultimap.builder();
    for (PTransform ptransform : pipeline.getComponents().getTransformsMap().values()) {
      if (ptransform.getSubtransformsCount() == 0) {
        for (String produced : ptransform.getOutputsMap().values()) {
          
        }
      }
    }
    this.producers = producers.build();
    this.consumers = consumers.build();
  }

  RunnerApi.PTransform getProducer(RunnerApi.PCollection pcollection) {
    return producers.get(pcollection);
  }

  List<RunnerApi.PTransform> getAllConsumers(RunnerApi.PCollection pcollection) {
    return consumers.get(pcollection);
  }

  Set<RunnerApi.PTransform> getRootTransforms() {
    ImmutableSet.Builder<RunnerApi.PTransform> rootTransforms = ImmutableSet.builder();
    for (String rootId : pipeline.getRootTransformIdsList()) {
      rootTransforms.add(pipeline.getComponents().getTransformsOrThrow(rootId));
    }
    return rootTransforms.build();
  }
}
