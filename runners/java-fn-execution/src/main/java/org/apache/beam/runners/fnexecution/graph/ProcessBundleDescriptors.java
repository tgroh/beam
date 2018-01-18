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

package org.apache.beam.runners.fnexecution.graph;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptorOrBuilder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;

/**
 * Utilities for converting between a collection of components and an executable {@link
 * ProcessBundleDescriptor}.
 */
public class ProcessBundleDescriptors {
  private ProcessBundleDescriptors() {}

  public static ProcessBundleDescriptorOrBuilder containingTransforms(
      String id,
      RunnerApi.Components components,
      Iterable<String> ptransformIds,
      ApiServiceDescriptor dataEndpoint) {
    return new SubgraphBuilder(components)
        .populate(ptransformIds)
        .addGrpcDataNodes(dataEndpoint)
        .toProcessBundleDescriptor(id);
  }

  private static class SubgraphBuilder {
    private final RunnerApi.Components components;
    private final LoadingCache<String, Coder> coders =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, Coder>() {
                  @Override
                  public Coder load(String coderId) throws Exception {
                    Coder coder = components.getCodersOrThrow(coderId);
                    for (String componentCoderId : coder.getComponentCoderIdsList()) {
                      coders.get(componentCoderId);
                    }
                    return coder;
                  }
                });
    private final LoadingCache<String, WindowingStrategy> windowingStrategies =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, WindowingStrategy>() {
                  @Override
                  public WindowingStrategy load(String windowingStrategyId) throws Exception {
                    WindowingStrategy windowingStrategy =
                        components.getWindowingStrategiesOrThrow(windowingStrategyId);
                    coders.get(windowingStrategy.getWindowCoderId());
                    return windowingStrategy;
                  }
                });
    private final LoadingCache<String, PCollection> pcollections =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, PCollection>() {
                  @Override
                  public PCollection load(String pcollectionId) throws Exception {
                    PCollection pcollection = components.getPcollectionsOrThrow(pcollectionId);
                    windowingStrategies.get(pcollection.getWindowingStrategyId());
                    coders.get(pcollection.getCoderId());
                    return pcollection;
                  }
                });
    private final LoadingCache<String, Environment> environments = CacheBuilder.newBuilder().build(
        new CacheLoader<String, Environment>() {
          @Override
          public Environment load(String environmentId) throws Exception {
            return components.getEnvironmentsOrThrow(environmentId);
          }
        });
    private final LoadingCache<String, PTransform> ptransforms =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, PTransform>() {
                  @Override
                  public PTransform load(String transformId) throws Exception {
                    PTransform transform = components.getTransformsOrThrow(transformId);
                    checkArgument(
                        transform.getSubtransformsCount() == 0,
                        "Every %s in a %s should have no components",
                        PTransform.class.getSimpleName(),
                        ProcessBundleDescriptor.class.getSimpleName());
                    for (String inputId : transform.getInputsMap().values()) {
                      pcollections.get(inputId);
                    }
                    for (String outputId : transform.getOutputsMap().values()) {
                      pcollections.get(outputId);
                    }
                    // TODO: Load any closures from the payload? What is required here?
                    // TODO: That's the only way we can get environments, among others
                    return transform;
                  }
                });

    private SubgraphBuilder(RunnerApi.Components components) {
      this.components = components;
    }

    public SubgraphBuilder populate(Iterable<String> ptransformIds) {
      for (String transformId : ptransformIds) {
        try {
          ptransforms.get(transformId);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      return this;
    }

    public SubgraphBuilder addGrpcDataNodes(ApiServiceDescriptor dataEndpoint) {
      Set<String> producedPCollections = new HashSet<>();
      Set<String> consumedPCollections = new HashSet<>();
      for (PTransform ptransform : ptransforms.asMap().values()) {
        producedPCollections.addAll(ptransform.getOutputsMap().values());
        consumedPCollections.addAll(ptransform.getInputsMap().values());
      }
      for (String remoteReadId : Sets.difference(consumedPCollections, producedPCollections)) {

      }
      for (String remoteWriteId : Sets.difference(producedPCollections, consumedPCollections)) {

      }
      return this;
    }

    private void addGrpcReadNode(ApiServiceDescriptor dataEndpoint, String pcollectionId) {
      RemoteGrpcPortRead.readFromPort(dataEndpoint.getUrl(), pcollectionId).toPTransform();
    }

    private void addGrpcWriteNode() {

    }

    private String generateTransformId() {

    }

    private String generateCoderId() {
    }

    private ProcessBundleDescriptor toProcessBundleDescriptor(
        String id, @Nullable ApiServiceDescriptor stateApiEndpoint) {
      ProcessBundleDescriptor.newBuilder()
          .setId(id)
          .putAllTransforms(ptransforms.asMap())
          .putAllPcollections(pcollections.asMap())
          .putAllWindowingStrategies(windowingStrategies.asMap())
          .putAllCoders(coders.asMap())
          .setStateApiServiceDescriptor(stateApiEndpoint)
          .build();
    }
  }
}
