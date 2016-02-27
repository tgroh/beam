/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link CreateUnbounded}.
 */
public class CreateUnboundedTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void applyProducesUnboundedCollection() {
    TestPipeline p = TestPipeline.create();
    assertThat(
        p.apply(CreateUnbounded.<Object>forType(TypeDescriptor.of(Object.class)).finish())
            .isBounded(),
        equalTo(IsBounded.UNBOUNDED));
  }

  @Test
  public void advanceWatermarkBackwardsInTimeThrows() {
    CreateUnbounded.UnfinishedCreateUnbounded<Object> input =
        CreateUnbounded.forType(TypeDescriptor.of(Object.class));

    Instant firstWatermark = new Instant(224_655L);
    input.advanceWatermark(firstWatermark);

    Instant earlierWatermark = firstWatermark.minus(250L);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("The watermark must advance monotonically");
    thrown.expectMessage(String.format("currently %s", firstWatermark));
    thrown.expectMessage(String.format("got %s", earlierWatermark));
    input.advanceWatermark(earlierWatermark);
  }

  @Test
  public void advanceWatermarkToCurrentWatermarkSucceeds() {
    Instant watermark = new Instant(854_517L);
    CreateUnbounded.forType(TypeDescriptor.of(Integer.class))
        .advanceWatermark(watermark)
        .advanceWatermark(watermark)
        .finish();
  }

  @Test
  public void addElementsAndFinishProducesElementsAndFinishes() {
    TimestampedValue<Integer> one = TimestampedValue.of(1, new Instant(1L));
    TimestampedValue<Integer> two = TimestampedValue.of(2, new Instant(2L));
    TimestampedValue<Integer> en = TimestampedValue.of(14, new Instant(15L));
    TimestampedValue<Integer> plusOne = TimestampedValue.of(15, new Instant(26L));
    CreateUnbounded<Integer> input =
        CreateUnbounded.forType(TypeDescriptor.of(Integer.class))
            .addElements(one, two, en, plusOne)
            .finish();

    TestPipeline p = TestPipeline.create();
    DataflowAssert.that(p.apply(input)).containsInAnyOrder(2, 14, 1, 15);

    p.run();
  }

  @Test
  public void addElementsAndAdvanceWatermarkWithDroppingLateDataSucceeds() {
    TimestampedValue<Integer> one = TimestampedValue.of(1, new Instant(1L));
    TimestampedValue<Integer> two = TimestampedValue.of(2, new Instant(2L));
    TimestampedValue<Integer> en =
        TimestampedValue.of(14, new Instant(0L).plus(Duration.standardDays(2)));
    TimestampedValue<Integer> plusOne =
        TimestampedValue.of(15, new Instant(0L).plus(Duration.standardDays(2)));
    CreateUnbounded<Integer> input =
        CreateUnbounded.forType(TypeDescriptor.of(Integer.class))
            .addElements(one, two, en)
            .advanceWatermark(new Instant(0L).plus(Duration.standardDays(3)))
            .addElements(plusOne)
            .finish();

    TestPipeline p = TestPipeline.create();
    PCollection<Integer> outputValues =
        p.apply(input)
            .setTypeDescriptorInternal(new TypeDescriptor<Integer>() {})
            .apply(
                Window.<Integer>into(FixedWindows.of(Duration.standardDays(1)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes())
            .apply(WithKeys.<String, Integer>of("foo"))
            .apply(GroupByKey.<String, Integer>create())
            .apply(Values.<Iterable<Integer>>create())
            .apply(Flatten.<Integer>iterables());

    DataflowAssert.that(outputValues).containsInAnyOrder(1, 2, 14);

    p.run();
  }

  @Test
  public void addElementsWithNoElementsSucceeds() {
    CreateUnbounded.forType(TypeDescriptor.of(String.class)).addElements();
  }
}
