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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link DirectTestSource}.
 */
@RunWith(JUnit4.class)
public class DirectTestSourceTest implements Serializable {

  @Test
  public void testLateData() {
    Instant instant = new Instant(0);
    DirectTestSource<Integer> source = DirectTestSource.create(new TypeDescriptor<Integer>() {
    })
        .addElements(TimestampedValue.of(1, instant),
            TimestampedValue.of(2, instant),
            TimestampedValue.of(3, instant))
        .advanceWatermarkTo(instant.plus(Duration.standardMinutes(20)))
        .addElements(TimestampedValue.of(-1, instant),
            TimestampedValue.of(-2, instant),
            TimestampedValue.of(-3, instant))
        .advanceWatermarkToInfinity();

    PCollection<Integer> windowed =
        source.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))).triggering(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(2)))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.standardMinutes(5)));
    PCollection<Long> count = windowed.apply(Count.<Integer>globally().withoutDefaults());
    PCollection<Integer> sum = windowed.apply(Sum.integersGlobally().withoutDefaults());

    PAssert.that(count)
        .inWindow(new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L))))
        .satisfies(new SerializableFunction<Iterable<Long>, Void>() {
          @Override
          public Void apply(Iterable<Long> input) {
            for (Long count : input) {
              System.out.println("Count: " + count);
              assertThat(count, greaterThanOrEqualTo(3L));
            }
            return null;
          }
        });
    PAssert.that(sum)
        .inWindow(new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L))))
        .satisfies(new SerializableFunction<Iterable<Integer>, Void>() {
          @Override
          public Void apply(Iterable<Integer> input) {
            for (Integer sum : input) {
              System.out.println("Sum: " + sum);
              assertThat(sum, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(6)));
            }
            return null;
          }
        });

    source.getPipeline().run();
  }

  @Test
  public void testAdvancesClock() {
    Instant instant = new Instant(0);
    DirectTestSource<Integer> source = DirectTestSource.create(new TypeDescriptor<Integer>() {
    })
        .addElements(TimestampedValue.of(1, instant),
            TimestampedValue.of(2, instant),
            TimestampedValue.of(3, instant))
        .advanceProcessingTime(Duration.standardMinutes(10L))
        .addElements(TimestampedValue.of(-1, instant),
            TimestampedValue.of(-2, instant),
            TimestampedValue.of(-3, instant))
        .advanceWatermarkToInfinity();

    PCollection<Integer> windowed =
        source.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))).triggering(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(2))))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.standardMinutes(5)));
    PCollection<Long> count = windowed.apply(Count.<Integer>globally().withoutDefaults());
    PCollection<Integer> sum = windowed.apply(Sum.integersGlobally().withoutDefaults());

    PAssert.that(count).satisfies(new SerializableFunction<Iterable<Long>, Void>() {
      @Override
      public Void apply(Iterable<Long> input) {
        for (Long count : input) {
          System.out.println("Count: " + count);
          assertThat(count, greaterThan(3L));
        }
        return null;
      }
    });
    PAssert.that(sum).satisfies(new SerializableFunction<Iterable<Integer>, Void>() {
      @Override
      public Void apply(Iterable<Integer> input) {
        for (Integer sum : input) {
          System.out.println("Sum: " + sum);
          assertThat(sum, allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(6)));
        }
        return null;
      }
    });

    source.getPipeline().run();
  }

  @Test
  public void testProcessingTimeTrigger() {
    DirectTestSource<Long> source = DirectTestSource.create(TypeDescriptor.of(Long.class))
        .addElements(TimestampedValue.of(1L, new Instant(1000L)),
            TimestampedValue.of(2L, new Instant(2000L)))
        .advanceProcessingTime(Duration.standardMinutes(12))
        .addElements(TimestampedValue.of(3L, new Instant(3000L)))
        .advanceWatermarkToInfinity();

    PCollection<Long> sum =
        source.apply(Window.<Long>triggering(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5))).accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO)).apply(Sum.longsGlobally());

    PAssert.that(sum).satisfies(new SerializableFunction<Iterable<Long>, Void>() {
      @Override
      public Void apply(Iterable<Long> input) {
        assertThat(input, hasItem(6L));
        System.out.println(input);
        return null;
      }
    });

    sum.getPipeline().run();
  }
}
