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

package org.apache.beam.runners.direct.portable;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Struct;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link PortableDirectRunner}. */
@RunWith(JUnit4.class)
public class PortableDirectRunnerTest implements Serializable {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void pipelineExecution() throws Exception {
    Pipeline p = Pipeline.create();
    TupleTag<KV<String, Integer>> food = new TupleTag<>();
    TupleTag<Integer> originals = new TupleTag<Integer>() {};
    PCollectionTuple parDoOutputs =
        p.apply(Create.of(1, 2, 3))
            .apply(
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void process(ProcessContext ctxt) {
                            for (int i = 0; i < ctxt.element(); i++) {
                              ctxt.outputWithTimestamp(
                                  KV.of("foo", i), new Instant(0).plus(Duration.standardHours(i)));
                            }
                            ctxt.output(originals, ctxt.element());
                          }
                        })
                    .withOutputTags(food, TupleTagList.of(originals)));
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(5L));
    PCollection<KV<String, Set<Integer>>> grouped =
        parDoOutputs
            .get(food)
            .apply(Window.into(windowFn))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<Integer>>, KV<String, Set<Integer>>>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output(
                            KV.of(
                                ctxt.element().getKey(),
                                ImmutableSet.copyOf(ctxt.element().getValue())));
                      }
                    }));

    // PCollection<Integer> otherPc = p.apply(Create.of(-1, -2, -3, -4));
    // PCollection<Integer> sums =
    //     PCollectionList.of(parDoOutputs.get(originals))
    //         .and(otherPc)
    //         .apply(Flatten.pCollections())
    //         .apply(Sum.integersGlobally());

    // PAssert.that(sums).containsInAnyOrder(-4);
    //
    // PAssert.that(grouped)
    //     .inWindow(windowFn.assignWindow(new Instant(0)))
    //     .containsInAnyOrder(KV.of("foo", ImmutableSet.of(0, 0, 0)));
    // PAssert.that(grouped)
    //     .inWindow(windowFn.assignWindow(new Instant(0).plus(Duration.standardHours(1L))))
    //     .containsInAnyOrder(KV.of("foo", ImmutableSet.of(1, 1)));
    // PAssert.that(grouped)
    //     .inWindow(windowFn.assignWindow(new Instant(0).plus(Duration.standardHours(2L))))
    //     .containsInAnyOrder(KV.of("foo", ImmutableSet.of(2)));
    // PAssert.that(grouped)
    //     .containsInAnyOrder(
    //         // Each window is independent
    //         KV.of("foo", ImmutableSet.of(0, 0, 0)),
    //         KV.of("foo", ImmutableSet.of(1, 1)),
    //         KV.of("foo", ImmutableSet.of(2)));

    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    PortableDirectRunner runner =
        PortableDirectRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p), Struct.getDefaultInstance(), tempFolder.getRoot());
    runner.execute();
  }
}
