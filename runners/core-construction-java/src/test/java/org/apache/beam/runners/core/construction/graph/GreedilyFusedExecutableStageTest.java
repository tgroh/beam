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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GreedilyFusedExecutableStage}. */
@RunWith(JUnit4.class)
public class GreedilyFusedExecutableStageTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final PCollection readDotOut = PCollection.newBuilder().setUniqueName("read.out").build();
  private final PCollectionNode readOutputNode = PipelineNode.pcollection("read.out", readDotOut);

  @Test
  public void noInitialConsumersThrows() {
    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "read",
                    PTransform.newBuilder()
                        .putOutputs("output", "read.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.READ_TRANSFORM_URN))
                        .build())
                .putPcollections("read.out", readDotOut)
                .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("at least one PTransform");
    GreedilyFusedExecutableStage.forGrpcPortRead(p, readOutputNode, Collections.emptySet());
  }

  @Test
  public void differentEnvironmentsThrows() {
    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "read", PTransform.newBuilder().putOutputs("output", "read.out").build())
                .putPcollections("read.out", readDotOut)
                .putTransforms(
                    "goTransform",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "go.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("go"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections("go.out", PCollection.newBuilder().setUniqueName("go.out").build())
                .putTransforms(
                    "pyTransform",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "py.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("py"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections("py.out", PCollection.newBuilder().setUniqueName("py.out").build())
                .putEnvironments("go", Environment.newBuilder().setUrl("go").build())
                .putEnvironments("py", Environment.newBuilder().setUrl("py").build())
                .build());
    Set<PTransformNode> differentEnvironments = p.getPerElementConsumers(readOutputNode);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("go");
    thrown.expectMessage("py");
    thrown.expectMessage("same");
    GreedilyFusedExecutableStage.forGrpcPortRead(p, readOutputNode, differentEnvironments);
  }

  @Test
  public void noEnvironmentThrows() {
    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "read", PTransform.newBuilder().putOutputs("output", "read.out").build())
                .putPcollections("read.out", readDotOut)
                .putTransforms(
                    "runnerTransform",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN))
                        .putOutputs("output", "gbk.out")
                        .build())
                .putPcollections(
                    "gbk.out", PCollection.newBuilder().setUniqueName("gbk.out").build())
                .build());
    Set<PTransformNode> differentEnvironments = p.getPerElementConsumers(readOutputNode);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Environment must be populated");
    GreedilyFusedExecutableStage.forGrpcPortRead(p, readOutputNode, differentEnvironments);
  }

  @Test
  public void nonReadRootThrows() {
    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "rootParDo",
                    PTransform.newBuilder()
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("env"))
                                        .build()
                                        .toByteString()))
                        .putOutputs("output", "read.out")
                        .build())
                .putPcollections("read.out", readDotOut)
                .putEnvironments("env", Environment.newBuilder().setUrl("env").build())
                .build());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(PTransformTranslation.READ_TRANSFORM_URN);
    GreedilyFusedExecutableStage.forRootTransform(p, getOnlyElement(p.getRootTransforms()));
  }

  @Test
  public void fusesCompatibleEnvironmentsWithRead() {
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
                .build());

    ExecutableStage subgraph =
        GreedilyFusedExecutableStage.forRootTransform(p, getOnlyElement(p.getRootTransforms()));
    assertThat(subgraph.getMaterializedPCollections(), emptyIterable());
    assertThat(subgraph.getConsumedPCollection().isPresent(), is(false));
    assertThat(
        subgraph.toPTransform().getSubtransformsList(),
        containsInAnyOrder("read", "parDo", "window"));
  }

  @Test
  public void fusesCompatibleEnivronmentsReadingFromPCollection() {
    Environment env = Environment.newBuilder().setUrl("common").build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "read",
                    PTransform.newBuilder()
                        .putOutputs("output", "read.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                                .setPayload(
                                    ReadPayload.newBuilder()
                                        .setSource(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("rare"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections("read.out", readDotOut)
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("rare", Environment.newBuilder().setUrl("rare").build())
                .putEnvironments("common", env)
                .build());

    PCollectionNode readOutput =
        getOnlyElement(p.getProducedPCollections(getOnlyElement(p.getRootTransforms())));
    ExecutableStage subgraph =
        GreedilyFusedExecutableStage.forGrpcPortRead(
            p, readOutput, p.getPerElementConsumers(readOutput));
    assertThat(subgraph.getMaterializedPCollections(), emptyIterable());
    assertThat(subgraph.getConsumedPCollection().isPresent(), is(true));
    assertThat(subgraph.getConsumedPCollection().get(), equalTo(readOutput));
    assertThat(subgraph.getEnvironment(), equalTo(env));
    assertThat(
        subgraph.toPTransform().getSubtransformsList(), containsInAnyOrder("parDo", "window"));
  }

  @Test
  public void fusesFlatten() {
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform flattenTransform =
        PTransform.newBuilder()
            .putInputs("readInput", "read.out")
            .putInputs("parDoInput", "parDo.out")
            .putOutputs("output", "flatten.out")
            .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("flatten", flattenTransform)
                .putPcollections(
                    "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
                .build());

    ExecutableStage subgraph =
        GreedilyFusedExecutableStage.forRootTransform(p, getOnlyElement(p.getRootTransforms()));
    assertThat(subgraph.getMaterializedPCollections(), emptyIterable());
    assertThat(subgraph.getConsumedPCollection().isPresent(), is(false));
    assertThat(
        subgraph.toPTransform().getSubtransformsList(),
        containsInAnyOrder("read", "parDo", "flatten", "window"));
  }

  @Test
  public void fusesFlattenWithDifferentEnvironmentInputs() {
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform otherEnvRead =
        PTransform.newBuilder()
            .putOutputs("output", "envRead.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("rare"))
                            .build()
                            .toByteString()))
            .build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .putOutputs("output", "parDo.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    PTransform flattenTransform =
        PTransform.newBuilder()
            .putInputs("readInput", "read.out")
            .putInputs("parDoInput", "parDo.out")
            .putInputs("otherEnvInput", "envRead.out")
            .putOutputs("output", "flatten.out")
            .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
            .build();
    PTransform windowTransform =
        PTransform.newBuilder()
            .putInputs("input", "flatten.out")
            .putOutputs("output", "window.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms("envRead", otherEnvRead)
                .putPcollections(
                    "envRead.out", PCollection.newBuilder().setUniqueName("envRead.out").build())
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms("flatten", flattenTransform)
                .putPcollections(
                    "flatten.out", PCollection.newBuilder().setUniqueName("flatten.out").build())
                .putTransforms("window", windowTransform)
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", Environment.newBuilder().setUrl("common").build())
                .putEnvironments("rare", Environment.newBuilder().setUrl("rare").build())
                .build());

    ExecutableStage subgraph =
        GreedilyFusedExecutableStage.forRootTransform(p, getOnlyElement(p.getRootTransforms()));
    assertThat(subgraph.getMaterializedPCollections(), emptyIterable());
    assertThat(subgraph.getConsumedPCollection().isPresent(), is(false));
    assertThat(
        subgraph.toPTransform().getSubtransformsList(),
        containsInAnyOrder("read", "parDo", "flatten", "window"));

    // Flatten shows up in both of these subgraphs, but elements only go through a path to the
    // flatten once.
    ExecutableStage readFromOtherEnv =
        GreedilyFusedExecutableStage.forRootTransform(p, p.transformNode("envRead"));
    assertThat(
        readFromOtherEnv.getMaterializedPCollections(), contains(p.pCollectionNode("flatten.out")));
    assertThat(
        readFromOtherEnv.toPTransform().getSubtransformsList(),
        containsInAnyOrder("envRead", "flatten"));
  }

  @Test
  public void materializesWithDifferentEnvConsumer() {
    Environment env = Environment.newBuilder().setUrl("common").build();
    PTransform parDoTransform =
        PTransform.newBuilder()
            .putInputs("input", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .setDoFn(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .putOutputs("out", "parDo.out")
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms(
                    "read",
                    PTransform.newBuilder()
                        .putOutputs("output", "read.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                                .setPayload(
                                    ReadPayload.newBuilder()
                                        .setSource(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections("read.out", readDotOut)
                .putTransforms("parDo", parDoTransform)
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "parDo.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("rare"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putEnvironments("rare", Environment.newBuilder().setUrl("rare").build())
                .putEnvironments("common", env)
                .build());

    PCollectionNode readOutput =
        getOnlyElement(p.getProducedPCollections(getOnlyElement(p.getRootTransforms())));
    ExecutableStage subgraph =
        GreedilyFusedExecutableStage.forGrpcPortRead(
            p, readOutput, p.getPerElementConsumers(readOutput));
    assertThat(subgraph.getMaterializedPCollections(), emptyIterable());
    assertThat(subgraph.getConsumedPCollection().isPresent(), is(true));
    assertThat(subgraph.getConsumedPCollection().get(), equalTo(readOutput));
    assertThat(subgraph.getEnvironment(), equalTo(env));
    assertThat(subgraph.toPTransform().getSubtransformsList(), contains("parDo"));
  }

  @Test
  public void materializesWithDifferentEnvSibling() {
    Environment env = Environment.newBuilder().setUrl("common").build();
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();
    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms(
                    "parDo",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "parDo.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "window.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("rare"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("rare", Environment.newBuilder().setUrl("rare").build())
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = getOnlyElement(p.getRootTransforms());
    PCollectionNode readOutput = getOnlyElement(p.getProducedPCollections(readNode));
    ExecutableStage subgraph = GreedilyFusedExecutableStage.forRootTransform(p, readNode);
    assertThat(subgraph.getMaterializedPCollections(), contains(readOutput));
    assertThat(subgraph.toPTransform().getSubtransformsList(), contains("read"));
  }

  @Test
  public void materializesWithSideInputConsumer() {
    Environment env = Environment.newBuilder().setUrl("common").build();
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms(
                    "side_read",
                    PTransform.newBuilder().putOutputs("output", "side_read.out").build())
                .putPcollections(
                    "side_read.out",
                    PCollection.newBuilder().setUniqueName("side_read.out").build())
                .putTransforms(
                    "parDo",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putInputs("side_input", "side_read.out")
                        .putOutputs("output", "parDo.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                                .setPayload(
                                    ParDoPayload.newBuilder()
                                        .setDoFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                                        .putSideInputs("side_input", SideInput.getDefaultInstance())
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections(
                    "parDo.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putTransforms(
                    "window",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "window.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.WINDOW_TRANSFORM_URN)
                                .setPayload(
                                    WindowIntoPayload.newBuilder()
                                        .setWindowFn(
                                            SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                                        .build()
                                        .toByteString()))
                        .build())
                .putPcollections(
                    "window.out", PCollection.newBuilder().setUniqueName("window.out").build())
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = PipelineNode.ptransform("read", readTransform);
    PCollectionNode readOutput = getOnlyElement(p.getProducedPCollections(readNode));
    ExecutableStage subgraph = GreedilyFusedExecutableStage.forRootTransform(p, readNode);
    assertThat(subgraph.getMaterializedPCollections(), contains(readOutput));
    assertThat(subgraph.toPTransform().getSubtransformsList(), contains(readNode.getId()));
  }

  @Test
  public void materializesWithGroupByKeyConsumer() {
    Environment env = Environment.newBuilder().setUrl("common").build();
    PTransform readTransform =
        PTransform.newBuilder()
            .putOutputs("output", "read.out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                    .setPayload(
                        ReadPayload.newBuilder()
                            .setSource(SdkFunctionSpec.newBuilder().setEnvironmentId("common"))
                            .build()
                            .toByteString()))
            .build();

    QueryablePipeline p =
        QueryablePipeline.fromComponents(
            Components.newBuilder()
                .putTransforms("read", readTransform)
                .putPcollections("read.out", readDotOut)
                .putTransforms(
                    "gbk",
                    PTransform.newBuilder()
                        .putInputs("input", "read.out")
                        .putOutputs("output", "gbk.out")
                        .setSpec(
                            FunctionSpec.newBuilder()
                                .setUrn(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN))
                        .build())
                .putPcollections(
                    "gbk.out", PCollection.newBuilder().setUniqueName("parDo.out").build())
                .putEnvironments("common", env)
                .build());

    PTransformNode readNode = PipelineNode.ptransform("read", readTransform);
    PCollectionNode readOutput = getOnlyElement(p.getProducedPCollections(readNode));
    ExecutableStage subgraph = GreedilyFusedExecutableStage.forRootTransform(p, readNode);
    assertThat(subgraph.getMaterializedPCollections(), contains(readOutput));
    assertThat(subgraph.toPTransform().getSubtransformsList(), contains(readNode.getId()));
  }
}
