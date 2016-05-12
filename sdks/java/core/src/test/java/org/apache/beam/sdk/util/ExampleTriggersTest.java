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

package org.apache.beam.sdk.util;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by tgroh on 5/11/16.
 */
@RunWith(JUnit4.class)
public class ExampleTriggersTest {
  private TestPipeline p = TestPipeline.create();

  @Test
  public void testAccumulatingModeOutputsFinalPaneWithAllElements() {
    PCollection<Integer> left = p.apply(Create.of(1, 2));
    PCollection<Integer> right = p.apply(Create.of(3, 4, 5));
    PCollection<Integer> flattened =
        PCollectionList.of(left).and(right).apply(Flatten.<Integer>pCollections());
    PCollection<Integer> windowed =
        flattened.apply(Window.<Integer>triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterPane.elementCountAtLeast(2))).withAllowedLateness(Duration.ZERO)
            .accumulatingFiredPanes());
    PCollection<KV<Void, Iterable<Integer>>> grouped = windowed
        .apply(WithKeys.<Void, Integer>of((Void) null))
        .apply(GroupByKey.<Void, Integer>create());

    PAssert.that(grouped)
        .inOnTimePaneInWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(1, 2, 3, 4, 5);
    PAssert.that(grouped)
        .inFinalPaneInWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(1, 2, 3, 4, 5);
  }

  @Test
  public void testDiscardingModeOutputsAllInputsByFinalPane() {
    PCollection<Integer> left = p.apply(Create.of(1, 2));
    PCollection<Integer> right = p.apply(Create.of(3, 4, 5));
    PCollection<Integer> flattened =
        PCollectionList.of(left).and(right).apply(Flatten.<Integer>pCollections());
    PCollection<Integer> windowed =
        flattened.apply(Window.<Integer>triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterPane.elementCountAtLeast(2))).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());
    PCollection<KV<Void, Iterable<Integer>>> grouped = windowed
        .apply(WithKeys.<Void, Integer>of((Void) null))
        .apply(GroupByKey.<Void, Integer>create());

    PAssert.that(grouped)
        .overAllPanesInWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(1, 2, 3, 4, 5);
  }

  @Test
  public void testDiscardingModeOutputsAllValuesByFinalPane() {
    PCollection<Integer> left = p.apply(Create.of(1, -2, 3));
    PCollection<Integer> right = p.apply(Create.of(-4, 5, -6));
    PCollection<Integer> flattened =
        PCollectionList.of(left).and(right).apply(Flatten.<Integer>pCollections());
    PCollection<Integer> windowed =
        flattened.apply(Window.<Integer>triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterPane.elementCountAtLeast(2))).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());
    PCollection<Integer> summed = windowed.apply(Sum.integersGlobally());

    PAssert.that(summed)
        .overAllPanesInWindow(GlobalWindow.INSTANCE)
        .satisfies(new SerializableFunction<Iterable<Integer>, Boolean>() {
          @Override
          public Boolean apply(Iterable<Integer> input) {
            int totalSum = 0;
            for (int i : input) {
              totalSum += i;
            }
            return totalSum == (1 + 3 + 5) - (2 + 4 + 6);
          }
        });
  }

  @Test
  public void testMultiWindows() {
    PCollection<Integer> left = p.apply(Create.of(100, -200, 300));
    PCollection<Integer> right = p.apply(Create.of(-400, 500, -600));
    PCollection<Integer> flattened = PCollectionList.of(left)
        .and(right)
        .apply(Flatten.<Integer>pCollections())
        .apply(WithTimestamps.of(new SerializableFunction<Integer, Instant>() {
          @Override
          public Instant apply(Integer input) {
            return new Instant(input.longValue());
          }
        }));

    // All of the elements will be in the same two windows
    SlidingWindows windowFn = SlidingWindows.of(Duration.standardMinutes(1))
        .every(Duration.standardSeconds(30))
        .withOffset(Duration.millis(1000));
    PCollection<Integer> windowed =
        flattened
            .apply(Window.<Integer>into(windowFn).triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterPane.elementCountAtLeast(2)))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes());
    PCollection<Integer> summed = windowed.apply(Sum.integersGlobally());

    PAssert.that(summed)
        .overAllPanesInEachNonemptyWindow()
        .satisfies(new SerializableFunction<Iterable<Integer>, Boolean>() {
          @Override
          public Boolean apply(Iterable<Integer> input) {
            int totalSum = 0;
            for (int i : input) {
              totalSum += i;
            }
            return totalSum == 100 * ((1 + 3 + 5) - (2 + 4 + 6));
          }
        });
  }
}
