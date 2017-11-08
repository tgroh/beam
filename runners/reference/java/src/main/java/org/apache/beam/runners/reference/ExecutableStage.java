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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;

/**
 * Created by tgroh on 11/7/17.
 */
public class ExecutableStage {
  private final String id;
  private final Map<String, RunnerApi.PTransform> transforms;
  private final Map<String, RunnerApi.PCollection> pcollections;
  private final Map<String, RunnerApi.Coder> coders;
  private final Map<String, RunnerApi.WindowingStrategy> windowingStrategies;
  private final Map<String, RunnerApi.Environment> environments;

  public static ExecutableStage forTransforms(
      String id, Iterable<String> ptransforms, RunnerApi.Components components) {
    Map<String, RunnerApi.PTransform> transforms = new HashMap<>();
    Map<String, RunnerApi.PCollection> pcollections = new HashMap<>();
    Map<String, RunnerApi.Coder> coders = new HashMap<>();
    Map<String, RunnerApi.WindowingStrategy> windowingStrategies = new HashMap<>();
    Map<String, RunnerApi.Environment> environments = new HashMap<>();
    for (String transformId : ptransforms) {
      PTransform transform = components.getTransformsOrThrow(transformId);
      transforms.put(transformId, transform);
      for (String inputValue : transform.getInputsMap().values()) {
        pcollections.put(inputValue, components.getPcollectionsOrThrow(inputValue));
      }
    }
    return new ExecutableStage(
        id, transforms, pcollections, coders, windowingStrategies, environments);
  }

  private ExecutableStage(
      String id,
      Map<String, PTransform> transforms,
      Map<String, PCollection> pcollections,
      Map<String, Coder> coders,
      Map<String, WindowingStrategy> windowingStrategies,
      Map<String, Environment> environments) {
    this.id = id;
    this.transforms = transforms;
    this.pcollections = pcollections;
    this.coders = coders;
    this.windowingStrategies = windowingStrategies;
    this.environments = environments;
  }

  public BeamFnApi.ProcessBundleDescriptor toProcessBundleDescriptor(
      ApiServiceDescriptor stateApiServiceDescriptor) {
    BeamFnApi.ProcessBundleDescriptor.Builder descriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder();
    descriptor
        .setId(id)
        .putAllTransforms(transforms)
        .putAllPcollections(pcollections)
        .putAllCoders(coders)
        .putAllWindowingStrategies(windowingStrategies)
        .putAllEnvironments(environments)
        .setStateApiServiceDescriptor(stateApiServiceDescriptor)
        .build();
    return descriptor.build();
  }
}
