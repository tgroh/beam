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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/**
 * A Fuser that constructs a fused pipeline by fusing as many PCollections into a stage as possible.
 */
public class GreedyPCollectionFusers {
  private static final Map<String, FusibilityChecker> URN_FUSIBILITY_CHECKERS =
      ImmutableMap.<String, FusibilityChecker>builder()
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, GreedyPCollectionFusers::canFuseParDo)
          .put(PTransformTranslation.WINDOW_TRANSFORM_URN, GreedyPCollectionFusers::canFuseWindow)
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, GreedyPCollectionFusers::canFuseFlatten)
          .put(
              PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
              GreedyPCollectionFusers::canFuseGroupByKey)
          .build();
  private static final FusibilityChecker DEFAULT_FUSIBILITY_CHECKER =
      GreedyPCollectionFusers::unknownTransformFusion;

  // TODO: Migrate
  private static final Map<String, CompatibilityChecker> URN_COMPATIBILITY_CHECKERS =
      ImmutableMap.<String, CompatibilityChecker>builder()
          .put(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              GreedyPCollectionFusers::parDoCompatibility)
          .put(
              PTransformTranslation.WINDOW_TRANSFORM_URN,
              GreedyPCollectionFusers::compatibleEnvironments)
          .put(
              PTransformTranslation.FLATTEN_TRANSFORM_URN, GreedyPCollectionFusers::noCompatibility)
          .put(
              PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
              GreedyPCollectionFusers::noCompatibility)
          .build();
  private static final CompatibilityChecker DEFAULT_COMPATIBILITY_CHECKER =
      GreedyPCollectionFusers::unknownTransformCompatibility;

  public static boolean canFuse(
      PTransformNode transformNode, ExecutableStage subgraph, QueryablePipeline pipeline) {
    return URN_FUSIBILITY_CHECKERS
        .getOrDefault(transformNode.getTransform().getSpec().getUrn(), DEFAULT_FUSIBILITY_CHECKER)
        .canFuse(transformNode, subgraph, pipeline);
  }

  public static boolean isCompatible(
      PTransformNode left, PTransformNode right, QueryablePipeline pipeline) {
    CompatibilityChecker leftChecker =
        URN_COMPATIBILITY_CHECKERS.getOrDefault(
            left.getTransform().getSpec().getUrn(), DEFAULT_COMPATIBILITY_CHECKER);
    CompatibilityChecker rightChecker =
        URN_COMPATIBILITY_CHECKERS.getOrDefault(
            right.getTransform().getSpec().getUrn(), DEFAULT_COMPATIBILITY_CHECKER);
    // The nodes are mutually compatible
    return leftChecker.isCompatible(left, right, pipeline)
        && rightChecker.isCompatible(right, left, pipeline);
  }

  // Alright, there are two things that the forward-scan fuser needs to do when it encounters a
  // PCollection:
  // If all of its consumers can be fused into the stage, fuse the PCollection and it's consumers,
  // adding all of the outputs of those consumers to the set of pending fusions.
  // If not all of its consumers can be fused into the stage, add a RemoteWrite node to the stage
  // as its only consumer, and determine the sets of transforms which are mutually compatible. Each
  // of those sets of transforms become the initial transforms of a new stage, reading from a
  // RemoteRead node which produces that PCollection.
/*  private Optional<Set<Set<PTransformNode>>> canFuse(
      PCollectionNode pCollectionNode, ExecutableStage subgraph) {
    Set<PTransformNode> happyCaseFusibleNodes = new HashSet<>();
    Set<Set<PTransformNode>> fusionBuckets = new HashSet<>();
    for (PTransformNode consumer : pipeline.getPerElementConsumers(pCollectionNode)) {
      boolean fused = false;
      if (fusionBuckets.isEmpty()) {
        FusibilityChecker fusibilityChecker = getFusibilityChecker(consumer);
        if (fusibilityChecker.canFuse(consumer, subgraph, pipeline)) {
          // The transform can be fused into this stage.
          happyCaseFusibleNodes.add(consumer);
          fused = true;
        } else {
          fusionBuckets.add(happyCaseFusibleNodes);
        }
      } else {
        for (Set<PTransformNode> compatibilityBucket : fusionBuckets) {
          if (allCompatible(consumer, compatibilityBucket)) {
            // The transform can be fused into the same subgraph as all of the members of the bucket
            // A transform can't be fused into more than one subgraph in this case, as doing so
            // would cause it to read the materialized PCollection from multiple ports.
            compatibilityBucket.add(consumer);
            fused = true;
            break;
          }
        }
      }
      if (!fused) {
        // There wasn't any compatible stage, so we have to place this transform in a new one.
        Set<PTransformNode> newBucket = new HashSet<>();
        newBucket.add(consumer);
        fusionBuckets.add(newBucket);
      }
    }
    if (fusionBuckets.isEmpty()) {
      // The PCollection isn't materialized (it's been fused away into the stage), so we have no
      // additional stages to add.
      return Optional.empty();
    } else {
      // The PCollection must be materialized, so return all of the mutually incompatible stages
      // that are downstream of it. Of note, the runner may not add all of the transforms to stages
      // (e.g., if one of the consumers is a Flatten or GroupByKey, they will not be placed in any
      // ExecutableStage.
      return Optional.of(fusionBuckets);
    }
  }

  private boolean allCompatible(PTransformNode consumer, Set<PTransformNode> compatibilityBucket) {
    for (PTransformNode bucketMember : compatibilityBucket) {
      // Compatibility goes both ways - no node can be compatible with any other node, so we check
      // both ways to ensure that order doesn't allow us to fuse something we're not allowed to.
      // This does take O(N**2) time where N is the number of consumers of the PCollection that
      // could not be fused, but that number is typically likely to be smol.
      if (!(getCompatibilityChecker(consumer).isCompatible(consumer, bucketMember, pipeline)
          && getCompatibilityChecker(bucketMember)
              .isCompatible(bucketMember, consumer, pipeline))) {
        return false;
      }
    }
    return true;
  }
  */

  // For the following methods, these should be called if the ptransform is consumed by a
  // PCollection output by the ExecutableStage, to determine if it can be fused into that
  // Subgraph

  private interface FusibilityChecker {
    boolean canFuse(
        PTransformNode consumer, ExecutableStage subgraph, QueryablePipeline pipeline);
  }

  private interface CompatibilityChecker {
    boolean isCompatible(
        PTransformNode newNode, PTransformNode otherNode, QueryablePipeline pipeline);
  }

  /**
   * A ParDo can be fused into a stage if it executes in the same Environment as that stage, and no
   * transform that are upstream of any of its side input are present in that stage.
   *
   * <p>A ParDo that consumes a side input cannot process an element until all of the side inputs
   * contain data for the Side Input window that contains the element.
   */
  private static boolean canFuseParDo(
      PTransformNode parDo, ExecutableStage stage, QueryablePipeline pipeline) {
    Optional<Environment> env = pipeline.getEnvironment(parDo);
    checkArgument(
        env.isPresent(),
        "A %s must have an %s associated with it",
        ParDoPayload.class.getSimpleName(),
        Environment.class.getSimpleName());
    if (!env.get().equals(stage.getEnvironment())) {
      // The PCollection's producer and this ParDo execute in different environments, so fusion
      // is never possible.
      return false;
    } else if (!pipeline.getSideInputs(parDo).isEmpty()) {
      // At execution time, a Runner is required to only provide inputs to a PTransform that, at the
      // time the PTransform processes them, the associated window is ready in all Side Inputs that
      // the PTransform consumes. For an arbitrary stage, it is significantly complex for the runner
      // to determine this for each input. As a result, we break fusion to simplify this inspection.
      // In general, a ParDo which consumes side inputs cannot be fused into an executable subgraph
      // alongside any transforms which are upstream of any of its side inputs.
      return false;
    }
    return true;
  }

  private static boolean parDoCompatibility(
      PTransformNode parDo, PTransformNode other, QueryablePipeline pipeline) {
    if (!pipeline.getSideInputs(parDo).isEmpty()) {
      // This is a convenience rather than a strict requirement. In general, a ParDo that consumes
      // side inputs can be fused with other transforms in the same environment which are not
      // upstream of any of the side inputs.
      return false;
    }
    return compatibleEnvironments(parDo, other, pipeline);
  }

  /**
   * A WindowInto can be fused into a stage if it executes in the same Environment as that stage.
   */
  private static boolean canFuseWindow(
      PTransformNode window, ExecutableStage stage, QueryablePipeline pipeline) {
    // WindowInto transforms may not have an environment
    Optional<Environment> windowEnvironment = pipeline.getEnvironment(window);
    return stage.getEnvironment().equals(windowEnvironment.orElse(null));
  }

  private static boolean compatibleEnvironments(
      PTransformNode left,
      PTransformNode right,
      QueryablePipeline pipeline) {
    return pipeline.getEnvironment(left).equals(pipeline.getEnvironment(right));
  }

  /**
   * A Flatten can be fused into a stage if all of the immediately upstream transforms execute in
   * the same environment.
   *
   * <p>TODO: Word this in a way that is actually compelling, and then test the heck out of it.
   *
   * <p>Within a stage, a {@link PCollection} is either fused away (and its elements are never
   * materialized), or materialized (and returned to the runner for later scheduling). Different
   * stages must be able to produce outputs to the same PCollection (for example, a PTransform which
   * is upstream of a side input and a PTransform which consumes that side input can be flattened
   * together, but these two transforms cannot be fused, and to know that we have to materialize
   * both of their output, we would have to inspect all of the inputs for all of the consuming
   * transforms, and I find this to be an unreasonable thing.
   *
   * <p>However, because the graph is a DAG, I am convinced that we are not hugely concerned with
   * this style of physical topology. Paths an element takes throughout a pipeline must ensure that
   * the element is consumed only once in each path it can take through the graph. I need to do the
   * legwork to demonstrate that this property is maintained in all cases, but effectively my
   * brain's heuristic states something among the lines of: if, prior to a flatten, each element is
   * produced in exactly one stage, if that fatten is fused into each of those stages, each of those
   * stages (again) will either fuse the flatten's output or will materialize it. For stages in
   * which it is fused away, it is never materialized to the runner (so it can't be re-consumed by
   * the downstream tree) For stages in which it is materialized, it is consumed by the This does
   * mean that each (Materialized PCollection Consumer x Environment) must be in EXACTLY ONE
   * subgraph, as otherwise it may receive elements for that PCollection in separate subgraphs and
   * be evaluated over an element more than once. I have no concern that this will not be the case.
   *
   * <p>Ultimately this boils down to "Prune each executable subgraph for unreachable per-element
   * nodes so the harness doesn't crash", which includes removing flatten inputs which aren't
   * produced within the stage.
   */
  private static boolean canFuseFlatten(
      @SuppressWarnings("unused") PTransformNode flatten,
      @SuppressWarnings("unused") ExecutableStage stage,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    // The flatten will be pruned within the subgraph, and it's output may be immediately
    // materialized as the output of the stage. This is partial flatten unzipping, I believe.
    return true;
  }

  private static boolean canFuseGroupByKey(
      @SuppressWarnings("unused") PTransformNode gbk,
      @SuppressWarnings("unused") ExecutableStage stage,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    // GroupByKeys are runner-implemented only. PCollections consumed by a GroupByKey must be
    // materialized.
    return false;
  }

  private static boolean noCompatibility(
      @SuppressWarnings("unused") PTransformNode self,
      @SuppressWarnings("unused") PTransformNode other,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    // TODO: There is performance to be gained if the output of a flatten is fused into a stage
    // where its output is wholly consumed after a fusion break. This requires slightly more
    // lookahead.
    return false;
  }

  private static boolean unknownTransformFusion(
      PTransformNode transform,
      @SuppressWarnings("unused") ExecutableStage stage,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    throw new IllegalArgumentException(
        String.format("Unknown URN %s", transform.getTransform().getSpec().getUrn()));
  }

  private static boolean unknownTransformCompatibility(
      PTransformNode transform,
      @SuppressWarnings("unused") PTransformNode other,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    throw new IllegalArgumentException(
        String.format(
            "Unknown or unsupported URN %s", transform.getTransform().getSpec().getUrn()));
  }
}
