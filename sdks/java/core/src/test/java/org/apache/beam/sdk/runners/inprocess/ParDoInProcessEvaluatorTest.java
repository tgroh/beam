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
package org.apache.beam.sdk.runners.inprocess;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.Bound;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests for {@link ParDoInProcessEvaluator}.
 */
@RunWith(JUnit4.class)
public class ParDoInProcessEvaluatorTest {
  private static Map<DoFn<Integer, Void>, Integer> timesExecuted = new ConcurrentHashMap<>();

  @Mock private InProcessEvaluationContext evaluationContext;
  private CommittedBundle<Integer> inputBundle;
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    timesExecuted = new ConcurrentHashMap<>();
  }

  private ParDoInProcessEvaluator<Integer> createEvaluator(DoFn<Integer, Void> fn) {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.<Integer>of(1, 2, 3));
    PCollection<Void> pardone = created.apply(ParDo.of(fn));
    AppliedPTransform<PCollection<Integer>, PCollection<Void>, ParDo.Bound<Integer, Void>>
        application =
            (AppliedPTransform<PCollection<Integer>, PCollection<Void>, Bound<Integer, Void>>)
                pardone.getProducingTransformInternal();
    TupleTag<Void> mainOutputTag = new TupleTag<Void>();

    when(evaluationContext.createBundle(inputBundle, pardone)).thenReturn(null);

    return ParDoInProcessEvaluator.create(
        evaluationContext,
        inputBundle,
        application,
        fn,
        Collections.<PCollectionView<?>>emptyList(),
        mainOutputTag,
        Collections.<TupleTag<?>>emptyList(),
        ImmutableMap.<TupleTag<?>, PCollection<?>>builder().put(mainOutputTag, pardone).build());
  }

  @Test
  public void shouldExecuteCopyOfFn() {
    DoFn<Integer, Void> original =
        new DoFn<Integer, Void>() {
          @Override
          public void processElement(DoFn<Integer, Void>.ProcessContext c) throws Exception {
            Integer previousValue = timesExecuted.putIfAbsent(this, 0);
            if (previousValue != null) {
              fail("Only expecting to process a single element in this instance.");
            }
          }
        };
    ParDoInProcessEvaluator<Integer> evaluator = createEvaluator(original);
    evaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    evaluator.finishBundle();
    assertThat(timesExecuted, hasValue(0));
    assertThat(timesExecuted.keySet(), hasSize(1));
    assertThat(timesExecuted, not(hasKey(original)));
  }

  @Test
  public void shouldExecuteSameFnInSameThreadWithSameInstance() {
    DoFn<Integer, Void> original =
        new DoFn<Integer, Void>() {
          @Override
          public void processElement(DoFn<Integer, Void>.ProcessContext c) throws Exception {
            timesExecuted.putIfAbsent(this, 0);
            timesExecuted.put(this, timesExecuted.get(this) + 1);
          }
        };
    ParDoInProcessEvaluator<Integer> evaluator = createEvaluator(original);
    evaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    evaluator.finishBundle();
    assertThat(timesExecuted, hasValue(0));
    assertThat(timesExecuted.keySet(), hasSize(1));
    assertThat(timesExecuted, not(hasKey(original)));
  }

  @Test
  public void shouldExecuteDifferentFnsInDifferentThreads() {}
}
