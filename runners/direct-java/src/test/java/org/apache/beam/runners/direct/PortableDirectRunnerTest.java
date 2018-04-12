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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.direct.PortableDirectRunner.PortableGroupByKeyReplacer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link PortableDirectRunner}. */
@RunWith(JUnit4.class)
public class PortableDirectRunnerTest implements Serializable {
  @Test
  public void replaceGroupByKeysWithGBKOToGABW() {
    Pipeline javaPipeline = Pipeline.create();
    javaPipeline
        .apply("Impulse", Impulse.create())
        .apply(
            "ParDo",
            ParDo.of(
                new DoFn<byte[], KV<String, Integer>>() {
                  @ProcessElement
                  public void output(ProcessContext ctxt) {
                    ctxt.output(KV.of("foo", 0));
                    ctxt.output(KV.of("foo", 1));
                  }
                }))
        .apply("GBK", GroupByKey.create());
    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(javaPipeline);

    RunnerApi.Pipeline updatedPipeline =
        ProtoOverrides.updateCompositesFor(
            PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
            pipeline,
            new PortableGroupByKeyReplacer());

    RunnerApi.PTransform updatedGbk = updatedPipeline.getComponents().getTransformsOrThrow("GBK");

    assertThat(updatedGbk.getSubtransformsList(), hasSize(2));

    RunnerApi.PTransform gbko =
        updatedPipeline.getComponents().getTransformsOrThrow(updatedGbk.getSubtransforms(0));
    RunnerApi.PTransform gabw =
        updatedPipeline.getComponents().getTransformsOrThrow(updatedGbk.getSubtransforms(1));

    assertThat(gbko.getSubtransformsList(), empty());
    assertThat(gabw.getSubtransformsList(), empty());

    assertThat(gbko.getSpec().getUrn(), equalTo(DirectGroupByKey.DIRECT_GBKO_URN));
    assertThat(gabw.getSpec().getUrn(), equalTo(DirectGroupByKey.DIRECT_GABW_URN));
  }
}
