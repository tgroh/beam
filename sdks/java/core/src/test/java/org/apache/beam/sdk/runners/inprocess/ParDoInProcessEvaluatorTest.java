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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    DoFn<Integer, Void> original = new CountNumInstanceExecutionsDoFn();
    ParDoInProcessEvaluator<Integer> evaluator = createEvaluator(original);
    evaluator.processElement(WindowedValue.valueInGlobalWindow(1));
    evaluator.finishBundle();
    assertThat(timesExecuted, hasValue(0));
    assertThat(timesExecuted.keySet(), hasSize(1));
    assertThat(timesExecuted, not(hasKey(original)));
  }

  @Test
  public void shouldExecuteSameFnInSameThreadWithSameInstance() throws Exception {
    DoFn<Integer, Void> original = new CountNumInstanceExecutionsDoFn();
    final ParDoInProcessEvaluator<Integer> evaluator = createEvaluator(original);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(getRunnable(evaluator, new CountDownLatch(0)));
    executor.execute(getRunnable(evaluator, new CountDownLatch(0)));
    executor.awaitTermination(500L, TimeUnit.MILLISECONDS);

    assertThat(timesExecuted, hasValue(2));
    assertThat(timesExecuted.keySet(), hasSize(1));
    assertThat(timesExecuted, not(hasKey(original)));
  }

  @Test
  public void shouldExecuteDifferentFnsInDifferentThreads() {
    DoFn<Integer, Void> original = new CountNumInstanceExecutionsDoFn();
    int numInstances = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numInstances);

    runInstances(original, numInstances, executor);

    // ran copies of the fn 5 times
    assertThat(timesExecuted.values(), contains(1, 1, 1, 1, 1));
    assertThat(timesExecuted, not(hasKey(original)));
  }

  @Test
  public void shouldReuseFnCopies() {
    DoFn<Integer, Void> original = new CountNumInstanceExecutionsDoFn();
    int numInstances = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numInstances);

    runInstances(original, numInstances, executor);

    // ran the same fn 5 times
    assertThat(timesExecuted.values(), contains(1, 1, 1, 1, 1));
    assertThat(timesExecuted, not(hasKey(original)));

    // 3 of the instances were reused, 2 were not, because they're using the same threads.
    runInstances(original, 3, executor);
    assertThat(timesExecuted.values(), containsInAnyOrder(2, 2, 2, 1, 1));
  }

  @Test
  public void shouldDiscardAfterExceptionInStartBundle() {}

  @Test
  public void shouldDiscardAfterExceptionInProcessElement() {}

  @Test
  public void shouldDiscardAfterExceptionInFinishBundle() {}

  private void runInstances(
      DoFn<Integer, Void> original, int numInstances, ExecutorService executor) {
    CountDownLatch startSignal = new CountDownLatch(1);
    for (int i = 0; i < numInstances; i++) {
      executor.submit(getRunnable(createEvaluator(original), startSignal));
    }
    startSignal.countDown();
  }

  private Runnable getRunnable(
      final ParDoInProcessEvaluator<Integer> evaluator, final CountDownLatch startSignal) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          boolean ran = startSignal.await(1L, TimeUnit.SECONDS);
          if (!ran) {
            fail("Timed out waiting for start signal to run");
          }
        } catch (InterruptedException e) {
          fail("Should not be interrupted");
        }
        evaluator.processElement(WindowedValue.valueInGlobalWindow(1));
        evaluator.finishBundle();
      }
    };
  }

  private static class CountNumInstanceExecutionsDoFn extends          DoFn<Integer, Void> {
    @Override
    public void processElement(DoFn<Integer, Void>.ProcessContext c) throws Exception {
      timesExecuted.putIfAbsent(this, 0);
      timesExecuted.put(this, timesExecuted.get(this) + 1);
    }
  }
}
