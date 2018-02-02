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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ExecutableStage} that will greedily fuse all available {@link PCollectionNode
 * PCollections} when it is constructed.
 *
 * <p>PCollections are either fully fused or fully materialized.
 */
public class GreedilyFusedExecutableStage implements ExecutableStage {
  // TODO: Provide a way to merge in a compatible subgraph (e.g. one where all of the siblings
  // consume a PCollection materialized by this subgraph and can be fused into it).
  private static final Logger LOG = LoggerFactory.getLogger(GreedilyFusedExecutableStage.class);

  /**
   * Returns an {@link ExecutableStage} where the initial {@link PTransformNode PTransform} is a
   * Read which executes within the environment (with no input elements or {@link PCollectionNode
   * PCollection}).
   *
   * <p>Once primitive Read transforms are replaced by an Impulse -> ParDo expansion, this
   * construction should be removed.
   */
  public static ExecutableStage forRootTransform(
      QueryablePipeline pipeline, PTransformNode rootTransform) {
    checkArgument(
        PTransformTranslation.READ_TRANSFORM_URN.equals(
            rootTransform.getTransform().getSpec().getUrn()),
        "Can only create an %s rooted at a %s node, got URN %s",
        ExecutableStage.class.getSimpleName(),
        PTransformTranslation.READ_TRANSFORM_URN,
        rootTransform.getTransform().getSpec().getUrn());
    return new GreedilyFusedExecutableStage(
        pipeline, null, Collections.singleton(rootTransform));
  }

  /**
   * Returns an {@link ExecutableStage} where the initial {@link PTransformNode PTransform} is a
   * Remote GRPC Port Read, reading elements from the materialized {@link PCollectionNode
   * PCollection}.
   *
   * @param initialNodes the initial set of sibling transforms to fuse into this node. All of the
   *     transforms must consume the {@code inputPCollection} on a per-element basis, and must all
   *     be mutually compatible.
   */
  public static ExecutableStage forGrpcPortRead(
      QueryablePipeline pipeline,
      PCollectionNode inputPCollection,
      Set<PTransformNode> initialNodes) {
    return new GreedilyFusedExecutableStage(pipeline, inputPCollection, initialNodes);
  }

  private final QueryablePipeline pipeline;
  private final Environment environment;

  @Nullable private final PCollectionNode maybeInputPCollection;

  private final Set<PCollectionNode> fusedCollections;
  private final Set<PTransformNode> fusedTransforms;
  private final Set<PCollectionNode> materializedPCollections;

  private final Queue<PCollectionNode> fusionCandidates;

  private GreedilyFusedExecutableStage(
      QueryablePipeline pipeline,
      @Nullable PCollectionNode inputPCollection,
      Set<PTransformNode> initialNodes) {
    checkArgument(
        !initialNodes.isEmpty(),
        "%s must contain at least one %s.",
        GreedilyFusedExecutableStage.class.getSimpleName(),
        PTransformNode.class.getSimpleName());
    // Choose the environment from an arbitrary node. The initial nodes may not be empty for this
    // subgraph to make any sense, there has to be at least one processor node
    // (otherwise the stage is GRPCRead -> GRPCWrite, which doesn't do anything).
    this.environment =
        pipeline
            .getEnvironment(initialNodes.iterator().next())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "%s must be populated on all %s in a %s",
                            Environment.class.getSimpleName(),
                            PTransformNode.class.getSimpleName(),
                            GreedilyFusedExecutableStage.class.getSimpleName())));
    initialNodes.forEach(
        transformNode ->
            checkArgument(
                environment.equals(pipeline.getEnvironment(transformNode).orElse(null)),
                "All %s in a %s must be the same. Got %s and %s",
                Environment.class.getSimpleName(),
                GreedilyFusedExecutableStage.class.getSimpleName(),
                environment,
                pipeline.getEnvironment(transformNode).orElse(null)));
    this.pipeline = pipeline;
    this.maybeInputPCollection = inputPCollection;
    this.fusedCollections = new HashSet<>();
    this.fusedTransforms = new LinkedHashSet<>(initialNodes);
    this.materializedPCollections = new HashSet<>();

    this.fusionCandidates = new ArrayDeque<>();
    for (PTransformNode initialConsumer : initialNodes) {
      fusionCandidates.addAll(pipeline.getOutputPCollections(initialConsumer));
    }
    while (!fusionCandidates.isEmpty()) {
      tryFuseCandiate(fusionCandidates.poll());
    }
  }

  private void tryFuseCandiate(PCollectionNode candidate) {
    if (materializedPCollections.contains(candidate) || fusedCollections.contains(candidate)) {
      // This should generally mean we get to a Flatten via multiple paths through the graph and
      // we've already determined what to do with the output.
      LOG.debug(
          "Skipping fusion candidate {} because it is {} already in this {}",
          candidate,
          materializedPCollections.contains(candidate) ? "materialized" : "fused",
          GreedilyFusedExecutableStage.class.getSimpleName());
      return;
    }
    boolean canFuse = true;
    for (PTransformNode node : pipeline.getPerElementConsumers(candidate)) {
      if (!(GreedyPCollectionFusers.canFuse(node, this, pipeline))) {
        canFuse = false;
        break;
      }
    }
    if (canFuse) {
      // All of the consumers of the candidate PCollection can be fused into this stage, so do so.
      fusedCollections.add(candidate);
      fusedTransforms.addAll(pipeline.getPerElementConsumers(candidate));
      for (PTransformNode consumer : pipeline.getPerElementConsumers(candidate)) {
        // The outputs of every transform fused into this stage must be either materialized or
        // themselves fused away, so add them to the set of candidates.
        fusionCandidates.addAll(pipeline.getOutputPCollections(consumer));
      }
    } else {
      // Some of the consumers can't be fused into this subgraph, so the PCollection has to be
      // materialized.
      // TODO: Potentially, some of the consumers can be fused back into this stage at a later point
      // complicate the process, but at a high level, if a downstream stage can be fused into all
      // of the stages that produce a PCollection it can be fused into all of those stages.
      materializedPCollections.add(candidate);
    }
  }

  @Override
  public Environment getEnvironment() {
    return environment;
  }

  @Override
  public Optional<PCollectionNode> getConsumedPCollection() {
    return Optional.ofNullable(maybeInputPCollection);
  }

  @Override
  public Collection<PCollectionNode> getMaterializedPCollections() {
    return Collections.unmodifiableSet(materializedPCollections);
  }

  @Override
  public Collection<PTransformNode> getTransforms() {
    return fusedTransforms;
  }

}
