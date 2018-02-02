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

package org.apache.beam.runners.core.construction.graph;

import static com.google.common.collect.Iterables.getOnlyElement;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage.Builder;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/**
 * A combination of PTransforms that can be executed within a single SDK harness.
 *
 * <p>Contains only the nodes that specify the processing to perform within the harness, and does
 * not contain any runner-executed nodes.
 *
 * <p>Within a single {@link Pipeline}, {@link PTransform PTransforms} and {@link PCollection
 * PCollections} are permitted to appear in multiple executable stages. However, paths from a root
 * {@link PTransform} to any other {@link PTransform} within that set of stages must be unique.
 */
public interface ExecutableStage {
  /**
   * The URN identifying an {@link ExecutableStage} that has been converted to a {@link PTransform}.
   */
  String URN = "urn:beam:runner:stage:v1";

  /**
   * Returns the {@link Environment} this stage executes in.
   *
   * <p>An {@link ExecutableStage} consists of {@link PTransform PTransforms} which can all be
   * executed within a single {@link Environment}. The assumption made here is that
   * runner-implemented transforms will be associated with these subgraphs by the overall graph
   * topology, which
   */
  Environment getEnvironment();

  /**
   * Returns the root {@link PCollectionNode} of this {@link ExecutableStage}. If the returned value
   * is present, this {@link ExecutableStage} executes by reading elements from a Remote GRPC Read
   * Node. If the returned value is absent, the {@link ExecutableStage} executes by reading from a
   * single Read node (which contains a {@link ReadTranslation read transform}).
   */
  Optional<PCollectionNode> getConsumedPCollection();

  /**
   * Returns the leaf {@link PCollectionNode PCollections} of this {@link ExecutableStage}. At
   * execution time, all of these {@link PCollectionNode PCollections} must be materialized by a
   * Remote GRPC Write Transform.
   */
  Collection<PCollectionNode> getMaterializedPCollections();

  /**
   * Returns the transforms executed in this {@link ExecutableStage}.
   */
  Collection<PTransformNode> getTransforms();

  /**
   * Returns a composite {@link PTransform} which contains as {@link
   * PTransform#getSubtransformsList() subtransforms} all of the {@link PTransform PTransforms}
   * fused into this {@link ExecutableStage}.
   *
   * <p>The input {@link PCollection} for the returned {@link PTransform} will be the consumed
   * {@link PCollectionNode} returned by {@link #getConsumedPCollection()} and the output {@link
   * PCollection PCollections} will be the {@link PCollectionNode PCollections} returned by {@link
   * #getMaterializedPCollections()}.
   */
  default PTransform toPTransform() {
    PTransform.Builder pt = PTransform.newBuilder();
    if (getConsumedPCollection().isPresent()) {
      pt.putInputs("input", getConsumedPCollection().get().getId());
    }
    int i = 0;
    for (PCollectionNode materializedPCollection : getMaterializedPCollections()) {
      pt.putOutputs(String.format("materialized_%s", i), materializedPCollection.getId());
      i++;
    }
    for (PTransformNode fusedTransform : getTransforms()) {
      // TODO: This may include nodes that have an input edge from multiple environments, which
      // could be problematic within the SDK harness, but also might not be
      pt.addSubtransforms(fusedTransform.getId());
    }
    pt.setSpec(FunctionSpec.newBuilder().setUrn(ExecutableStage.URN));
    return pt.build();
  }

  static ExecutableStage fromPTransform(PTransform transform, Components components) {
    Builder stageBuilder = ImmutableExecutableStage.builder();
    if (transform.getInputsCount() == 0) {
      stageBuilder = stageBuilder.setConsumedPCollection(Optional.empty());
    } else if (transform.getInputsCount() == 1) {
      String input = getOnlyElement(transform.getInputsMap().values());
      stageBuilder =
          stageBuilder.setConsumedPCollection(
              Optional.of(
                  PipelineNode.pCollection(input, components.getPcollectionsOrThrow(input))));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unexpected number of inputs for %s, 0 or 1 permitted: %s",
              ExecutableStage.class.getSimpleName(), transform.getInputsMap()));
    }
    return stageBuilder
        .setMaterializedPCollections(
            transform
                .getOutputsMap()
                .values()
                .stream()
                .map(
                    materialized ->
                        PipelineNode.pCollection(
                            materialized, components.getPcollectionsOrThrow(materialized)))
                .collect(Collectors.toList()))
        .setTransforms(
            transform
                .getSubtransformsList()
                .stream()
                .map(
                    subTransform ->
                        PipelineNode.pTransform(
                            subTransform, components.getTransformsOrThrow(subTransform)))
                .collect(Collectors.toList()))
        .setEnvironment(
            Environments.getEnvironment(
                    components.getTransformsOrThrow(transform.getSubtransforms(0)),
                    RehydratedComponents.forComponents(components))
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            String.format(
                                "Subtransform %s has no %s",
                                transform.getSubtransforms(0), Environment.class.getSimpleName()))))
        .build();
  }
}
