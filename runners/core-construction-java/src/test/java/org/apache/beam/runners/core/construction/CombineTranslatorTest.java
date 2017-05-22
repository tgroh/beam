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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.CombinePayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineIntegerFn;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link CombineTranslator}.
 */
@RunWith(Parameterized.class)
public class CombineTranslatorTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<Combine.CombineFn<Integer, ?, ?>> params() {
    BinaryCombineIntegerFn sum = Sum.ofIntegers();
    CombineFn<Integer, ?, Long> count = Count.combineFn();
    TestCombineFn test = new TestCombineFn();
    ImmutableList<CombineFn<Integer, ?, ?>> params =
        ImmutableList.<CombineFn<Integer, ?, ?>>builder()
            .add(sum)
            .add(count)
            .add(test)
            .build();
    return params;
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Parameter(0)
  public Combine.CombineFn<Integer, ?, ?> combineFn;

  @Test
  public void testToFromProto() throws Exception {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3));
    input.apply(Combine.globally(combineFn));
    final AtomicReference<AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>>> combine =
        new AtomicReference<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.getTransform() instanceof Combine.PerKey) {
              checkState(combine.get() != null);
              combine.set((AppliedPTransform) node.toAppliedPTransform());
            }
          }
        });
    checkState(combine.get() != null);

    SdkComponents sdkComponents = SdkComponents.create();
    CombinePayload combineProto = CombineTranslator.toProto(combine.get(), sdkComponents);
    RunnerApi.Components componentsProto = sdkComponents.toComponents();

    assertEquals(
        combineFn.getAccumulatorCoder(pipeline.getCoderRegistry(), input.getCoder()),
        CombineTranslator.getAccumulatorCoder(combineProto, componentsProto));
    assertEquals(
        Matchers.<GlobalCombineFn<?, ?, ?>>equalTo(combineFn),
        CombineTranslator.getCombineFn(combineProto));
  }

  private static class TestCombineFn extends Combine.CombineFn<Integer, Object, Object> {
    @Override
    public Object createAccumulator() {
      return new Object();
    }

    @Override
    public Coder<Object> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return (Coder) VoidCoder.of();
    }

    @Override
    public Object extractOutput(Object accumulator) {
      return accumulator;
    }

    @Override
    public Object mergeAccumulators(Iterable<Object> accumulators) {
      return new Object();
    }

    @Override
    public Object addInput(Object accumulator, Integer input) {
      return accumulator;
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other.getClass().equals(TestCombineFn.class);
    }

    @Override
    public int hashCode() {
      return TestCombineFn.class.hashCode();
    }
  }
}
