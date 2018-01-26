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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link QueryablePipeline}. */
@RunWith(JUnit4.class)
public class QueryablePipelineTest {
  @Test
  public void fromComponentsWithMalformedComponents() {}

  @Test
  public void rootTransforms() {
    Pipeline p = Pipeline.create();
    p.apply("UnboundedRead", Read.from(CountingSource.unbounded()))
        .apply(Window.into(FixedWindows.of(Duration.millis(5L))))
        .apply(Count.perElement());
    p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));

    Components components = PipelineTranslation.toProto(p).getComponents();
    QueryablePipeline qp = QueryablePipeline.fromComponents(components);

    assertThat(qp.getRootTransforms(), hasSize(2));
    for (PTransformNode rootTransform : qp.getRootTransforms()) {
      assertThat(
          "Root transforms should have no inputs",
          rootTransform.getTransform().getInputsCount(),
          equalTo(0));
      assertThat(
          "Only added source reads to the pipeline",
          rootTransform.getTransform().getSpec().getUrn(),
          equalTo(PTransformTranslation.READ_TRANSFORM_URN));
    }
  }

  /**
   * Tests that inputs that are only side inputs are not returned from {@link
   * QueryablePipeline#getPerElementConsumers(PCollectionNode)} and are returned from {@link
   * QueryablePipeline#getSideInputs(PTransformNode)}.
   */
  @Test
  public void transformWithSideAndMainInputs() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionView<String> view =
        p.apply("Create", Create.of("foo")).apply("View", View.asSingleton());
    longs.apply(
        "par_do",
        ParDo.of(new TestFn())
            .withSideInputs(view)
            .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    Components components = PipelineTranslation.toProto(p).getComponents();
    String mainInputName =
        Iterables.getOnlyElement(
            components.getTransformsOrThrow("BoundedRead").getOutputsMap().values());
    PCollectionNode mainInput =
        PipelineNode.pcollection(mainInputName, components.getPcollectionsOrThrow(mainInputName));
    String sideInputName =
        Iterables.getOnlyElement(
            components
                .getTransformsOrThrow("par_do")
                .getInputsMap()
                .values()
                .stream()
                .filter(pcollectionName -> !pcollectionName.equals(mainInputName))
                .collect(Collectors.toSet()));
    PCollectionNode sideInput =
        PipelineNode.pcollection(sideInputName, components.getPcollectionsOrThrow(sideInputName));
    PTransformNode parDoNode =
        PipelineNode.ptransform("par_do", components.getTransformsOrThrow("par_do"));
    QueryablePipeline qp = QueryablePipeline.fromComponents(components);
    assertThat(qp.getSideInputs(parDoNode), contains(sideInput));
    assertThat(qp.getPerElementConsumers(mainInput), contains(parDoNode));
    assertThat(qp.getPerElementConsumers(sideInput), not(contains(parDoNode)));
  }

  /**
   * Tests that inputs that are both side inputs and main inputs are returned from {@link
   * QueryablePipeline#getPerElementConsumers(PCollectionNode)} and {@link
   * QueryablePipeline#getSideInputs(PTransformNode)}.
   */
  @Test
  public void transformWithSameSideAndMainInput() {
    RunnerApi.PTransform root =
        RunnerApi.PTransform.newBuilder().putOutputs("out", "read_pc").build();
    RunnerApi.PTransform multiConsumer =
        RunnerApi.PTransform.newBuilder()
            .putInputs("main_in", "read_pc")
            .putInputs("side_in", "read_pc")
            .putOutputs("out", "pardo_out")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                    .setPayload(
                        ParDoPayload.newBuilder()
                            .putSideInputs("side_in", SideInput.getDefaultInstance())
                            .build()
                            .toByteString())
                    .build())
            .build();
    RunnerApi.PCollection readPC = RunnerApi.PCollection.getDefaultInstance();
    RunnerApi.PCollection pardoOut = RunnerApi.PCollection.getDefaultInstance();
    Components components =
        Components.newBuilder()
            .putPcollections("read_pc", readPC)
            .putPcollections("pardo_out", pardoOut)
            .putTransforms("root", root)
            .putTransforms("multiConsumer", multiConsumer)
            .build();

    QueryablePipeline qp = QueryablePipeline.fromComponents(components);
    PCollectionNode multiInputPc = PipelineNode.pcollection("read_pc", readPC);
    PTransformNode multiConsumerPT = PipelineNode.ptransform("multiConsumer", multiConsumer);
    assertThat(qp.getPerElementConsumers(multiInputPc), contains(multiConsumerPT));
    assertThat(qp.getSideInputs(multiConsumerPT), contains(multiInputPc));
  }

  /**
   * Tests that {@link QueryablePipeline#getPerElementConsumers(PCollectionNode)} returns a
   * transform that consumes the node more than once.
   */
  @Test
  public void perElementConsumersWithConsumingMultipleTimes() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionList.of(longs).and(longs).and(longs).apply("flatten", Flatten.pCollections());

    Components components = PipelineTranslation.toProto(p).getComponents();
    // This breaks if the way that IDs are assigned to PTransforms changes in PipelineTranslation
    String readOutput =
        Iterables.getOnlyElement(
            components.getTransformsOrThrow("BoundedRead").getOutputsMap().values());
    QueryablePipeline qp = QueryablePipeline.fromComponents(components);
    Set<PTransformNode> consumers =
        qp.getPerElementConsumers(
            PipelineNode.pcollection(readOutput, components.getPcollectionsOrThrow(readOutput)));

    assertThat(consumers.size(), equalTo(1));
    assertThat(
        Iterables.getOnlyElement(consumers).getTransform().getSpec().getUrn(),
        equalTo(PTransformTranslation.FLATTEN_TRANSFORM_URN));
  }

  @Test
  public void getProducer() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionList.of(longs).and(longs).and(longs).apply("flatten", Flatten.pCollections());

    Components components = PipelineTranslation.toProto(p).getComponents();
    String longsOutputName =
        Iterables.getOnlyElement(
            components.getTransformsOrThrow("BoundedRead").getOutputsMap().values());
    PTransformNode longsProducer =
        PipelineNode.ptransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"));
    PCollectionNode longsOutput =
        PipelineNode.pcollection(
            longsOutputName, components.getPcollectionsOrThrow(longsOutputName));
    String flattenOutputName =
        Iterables.getOnlyElement(
            components.getTransformsOrThrow("flatten").getOutputsMap().values());
    PTransformNode flattenProducer =
        PipelineNode.ptransform("flatten", components.getTransformsOrThrow("flatten"));
    PCollectionNode flattenOutput =
        PipelineNode.pcollection(
            flattenOutputName, components.getPcollectionsOrThrow(flattenOutputName));

    QueryablePipeline qp = QueryablePipeline.fromComponents(components);
    assertThat(qp.getProducer(longsOutput), equalTo(longsProducer));
    assertThat(qp.getProducer(flattenOutput), equalTo(flattenProducer));
  }

  @Test
  public void getEnvironmentWithEnvironment() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionList.of(longs).and(longs).and(longs).apply("flatten", Flatten.pCollections());

    Components components = PipelineTranslation.toProto(p).getComponents();
    PTransformNode environmentalRead =
        PipelineNode.ptransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"));
    PTransformNode nonEnvironmentalTransform =
        PipelineNode.ptransform("flatten", components.getTransformsOrThrow("flatten"));

    QueryablePipeline qp = QueryablePipeline.fromComponents(components);
    assertThat(qp.getEnvironment(environmentalRead).isPresent(), is(true));
    assertThat(
        qp.getEnvironment(environmentalRead).get(),
        equalTo(Environments.JAVA_SDK_HARNESS_ENVIRONMENT));
    assertThat(qp.getEnvironment(nonEnvironmentalTransform).isPresent(), is(false));
  }

  private static class TestFn extends DoFn<Long, Long> {
    @ProcessElement
    public void process(ProcessContext ctxt) {}
  }
}
