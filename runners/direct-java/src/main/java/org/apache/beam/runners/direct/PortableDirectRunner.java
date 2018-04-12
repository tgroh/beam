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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ComponentsOrBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.ModelCoders.KvCoderComponents;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides.TransformReplacement;
import org.apache.beam.runners.core.construction.graph.SyntheticNodes;

/** The "ReferenceRunner" engine implementation. */
class PortableDirectRunner {
  private final RunnerApi.Pipeline pipeline;

  private PortableDirectRunner(RunnerApi.Pipeline p) throws IOException {
    this.pipeline = executable(p);
  }

  static PortableDirectRunner forPipeline(RunnerApi.Pipeline p) throws IOException {
    return new PortableDirectRunner(p);
  }

  private RunnerApi.Pipeline executable(RunnerApi.Pipeline original) {
    RunnerApi.Pipeline withGbks =
        ProtoOverrides.updateCompositesFor(
            PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
            original,
            new PortableGroupByKeyReplacer());
    return GreedyPipelineFuser.fuse(withGbks).toPipeline();
  }

  public void execute() {
    ExecutableGraph<PTransformNode, PCollectionNode> graph = PortableGraph.forPipeline(pipeline);
    EvaluationContext ctxt = null;
    TransformEvaluatorRegistry transformRegistry = TransformEvaluatorRegistry.portableRegistry(ctxt);
    RootProviderRegistry rootRegistry = RootProviderRegistry.impulseRegistry(ctxt);
    int targetParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 3);
    ExecutorServiceParallelExecutor executor =
        ExecutorServiceParallelExecutor.create(
            targetParallelism, transformRegistry, Collections.emptyMap(), ctxt);
    executor.start();
  }

  @VisibleForTesting
  static class PortableGroupByKeyReplacer implements TransformReplacement {
    public MessageWithComponents getReplacement(String gbkId, ComponentsOrBuilder components) {
      PTransform gbk = components.getTransformsOrThrow(gbkId);
      checkArgument(
          PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN.equals(gbk.getSpec().getUrn()),
          "URN must be %s, got %s",
          PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
          gbk.getSpec().getUrn());
      String inputId = getOnlyElement(gbk.getInputsMap().values());
      PCollection input = components.getPcollectionsOrThrow(inputId);

      Coder inputCoder = components.getCodersOrThrow(input.getCoderId());
      KvCoderComponents kvComponents = ModelCoders.getKvCoderComponents(inputCoder);
      String windowCoderId =
          components
              .getWindowingStrategiesOrThrow(input.getWindowingStrategyId())
              .getWindowCoderId();
      // This coder isn't actually required for the pipeline to function properly - the KWIs can be
      // passed around as pure java objects with no coding of the values, but it approximates a full
      // pipeline.
      Coder intermediateCoder =
          Coder.newBuilder()
              .setSpec(
                  SdkFunctionSpec.newBuilder()
                      .setSpec(FunctionSpec.newBuilder().setUrn("beam:direct:keyedworkitem:v1")))
              .addAllComponentCoderIds(
                  ImmutableList.of(
                      kvComponents.keyCoderId(), kvComponents.valueCoderId(), windowCoderId))
              .build();
      String intermediateCoderId =
          SyntheticNodes.uniqueId(
              String.format(
                  "keyed_work_item(%s:%s)", kvComponents.keyCoderId(), kvComponents.valueCoderId()),
              components::containsCoders);

      String partitionedId =
          SyntheticNodes.uniqueId(
              String.format("%s.%s", inputId, "partitioned"), components::containsPcollections);
      // The partitioned PCollection has the same WindowingStrategy as the input, as no merging will
      // have been performed, so elements remain in their original windows
      PCollection partitioned =
          input.toBuilder().setUniqueName(partitionedId).setCoderId(intermediateCoderId).build();
      String gbkoId =
          SyntheticNodes.uniqueId(String.format("%s/GBKO", gbkId), components::containsTransforms);
      PTransform gbko =
          PTransform.newBuilder()
              .putAllInputs(gbk.getInputsMap())
              .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GBKO_URN))
              .putOutputs("output", partitionedId)
              .build();
      String gabwId =
          SyntheticNodes.uniqueId(String.format("%s/GABW", gbkId), components::containsTransforms);
      PTransform gabw =
          PTransform.newBuilder()
              .putInputs("input", partitionedId)
              .setSpec(FunctionSpec.newBuilder().setUrn(DirectGroupByKey.DIRECT_GABW_URN))
              .putAllOutputs(gbk.getOutputsMap())
              .build();
      Components newComponents =
          Components.newBuilder()
              .putCoders(intermediateCoderId, intermediateCoder)
              .putPcollections(partitionedId, partitioned)
              .putTransforms(gbkoId, gbko)
              .putTransforms(gabwId, gabw)
              .build();
      return MessageWithComponents.newBuilder()
          .setPtransform(gbk.toBuilder().addSubtransforms(gbkoId).addSubtransforms(gabwId).build())
          .setComponents(newComponents)
          .build();
    }
  }
}
