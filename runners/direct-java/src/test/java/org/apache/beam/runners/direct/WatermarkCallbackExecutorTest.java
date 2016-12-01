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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WatermarkCallbackExecutor}.
 */
@RunWith(JUnit4.class)
public class WatermarkCallbackExecutorTest {
  private WatermarkCallbackExecutor executor =
      WatermarkCallbackExecutor.create(Executors.newSingleThreadExecutor());
  private AppliedPTransform<?, ?, ?> createProducer;
  private AppliedPTransform<?, ?, ?> sumProducer;

  @Before
  public void setup() {
    TestPipeline p = TestPipeline.create();
    Values<Integer> createValues = Create.of(1, 2, 3);
    PCollection<Integer> created = p.apply("Create", createValues);
    createProducer = AppliedPTransform.of("Create", p.begin(), created, createValues);
    Combine.Globally<Integer, Integer> sumGlobally = Sum.integersGlobally();
    PCollection<Integer> theSum = created.apply("Sum", sumGlobally);
    sumProducer = AppliedPTransform.of("Sum", created, theSum, sumGlobally);
  }

  @Test
  public void onGuaranteedFiringFiresAfterTrigger() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    executor.callOnGuaranteedFiring(createProducer,
        GlobalWindow.INSTANCE,
        WindowingStrategy.globalDefault(),
        new CountDownLatchCallback(latch));

    executor.fireForWatermark(createProducer, BoundedWindow.TIMESTAMP_MAX_VALUE);
    assertThat(latch.await(500, TimeUnit.MILLISECONDS), equalTo(true));
  }

  @Test
  public void multipleCallbacksShouldFireFires() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    WindowFn<Object, IntervalWindow> windowFn = FixedWindows.of(Duration.standardMinutes(10));
    IntervalWindow window =
        new IntervalWindow(new Instant(0L), new Instant(0L).plus(Duration.standardMinutes(10)));
    executor.callOnGuaranteedFiring(
        createProducer, window, WindowingStrategy.of(windowFn), new CountDownLatchCallback(latch));
    executor.callOnGuaranteedFiring(
        createProducer, window, WindowingStrategy.of(windowFn), new CountDownLatchCallback(latch));

    executor.fireForWatermark(createProducer, new Instant(0L).plus(Duration.standardMinutes(10)));
    assertThat(latch.await(500, TimeUnit.MILLISECONDS), equalTo(true));
  }

  @Test
  public void noCallbacksShouldFire() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    WindowFn<Object, IntervalWindow> windowFn = FixedWindows.of(Duration.standardMinutes(10));
    IntervalWindow window =
        new IntervalWindow(new Instant(0L), new Instant(0L).plus(Duration.standardMinutes(10)));
    executor.callOnGuaranteedFiring(
        createProducer, window, WindowingStrategy.of(windowFn), new CountDownLatchCallback(latch));

    executor.fireForWatermark(createProducer, new Instant(0L).plus(Duration.standardMinutes(5)));
    assertThat(latch.await(500, TimeUnit.MILLISECONDS), equalTo(false));
  }

  @Test
  public void unrelatedStepShouldNotFire() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    WindowFn<Object, IntervalWindow> windowFn = FixedWindows.of(Duration.standardMinutes(10));
    IntervalWindow window =
        new IntervalWindow(new Instant(0L), new Instant(0L).plus(Duration.standardMinutes(10)));
    executor.callOnGuaranteedFiring(
        sumProducer, window, WindowingStrategy.of(windowFn), new CountDownLatchCallback(latch));

    executor.fireForWatermark(createProducer, new Instant(0L).plus(Duration.standardMinutes(20)));
    assertThat(latch.await(500, TimeUnit.MILLISECONDS), equalTo(false));
  }

  private static class CountDownLatchCallback implements Runnable {
    private final CountDownLatch latch;

    public CountDownLatchCallback(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void run() {
      latch.countDown();
    }
  }
}
