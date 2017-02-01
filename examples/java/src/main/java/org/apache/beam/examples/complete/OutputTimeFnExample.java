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

package org.apache.beam.examples.complete;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An example demonstrating how {@link OutputTimeFn}, {@link Trigger Triggers} and
 * {@link Window#accumulatingFiredPanes() accumulating mode} can cause a {@link Pipeline} to behave
 * in an unexpected manner.
 */
public class OutputTimeFnExample implements Serializable {

  public static final SerializableFunction<Long, Instant> MILLIS_SINCE_THE_EPOCH_TS_FN =
      new SerializableFunction<Long, Instant>() {
        @Override
        public Instant apply(Long input) {
          return new Instant(input);
        }
      };

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline p = Pipeline.create(options);
    PCollection<Long> longs =
        p.apply(
            CountingInput.unbounded()
                .withRate(100L, Duration.standardSeconds(1L))
                .withTimestampFn(MILLIS_SINCE_THE_EPOCH_TS_FN));

    PCollection<Long> windowedLongs =
        longs
            .apply(WithTimestamps.of(MILLIS_SINCE_THE_EPOCH_TS_FN))
            .apply(
                Window.<Long>into(FixedWindows.of(Duration.standardSeconds(1L)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(10)))
                    .withAllowedLateness(Duration.ZERO)
                    .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
                    .accumulatingFiredPanes());

    PCollection<Long> gbkedLongs =
        windowedLongs
            .apply(WithKeys.<Integer, Long>of(0).withKeyType(new TypeDescriptor<Integer>() {}))
            .apply(GroupByKey.<Integer, Long>create())
            .apply(Values.<Iterable<Long>>create())
            .apply(Flatten.<Long>iterables());

    gbkedLongs.apply(WithTimestamps.of(MILLIS_SINCE_THE_EPOCH_TS_FN));

    // Should crash in most cases
    p.run().waitUntilFinish();
  }

  public static void rewindowingDoesntWorkAsExpected(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline p = Pipeline.create(options);
    PCollection<Long> longs =
        p.apply(
            CountingInput.unbounded()
                .withRate(200L, Duration.standardSeconds(1L))
                .withTimestampFn(MILLIS_SINCE_THE_EPOCH_TS_FN));
    PCollection<KV<Long, Long>> keyed =
        longs
            .apply(WithTimestamps.of(MILLIS_SINCE_THE_EPOCH_TS_FN))
            .apply(
                WithKeys.of(
                    new SerializableFunction<Long, Long>() {
                      @Override
                      public Long apply(Long input) {
                        return input % 100;
                      }
                    }));

    PCollection<KV<Long, Long>> windowed =
        keyed.apply(
            Window.<KV<Long, Long>>into(Sessions.withGapDuration(Duration.standardSeconds(1L)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterPane.elementCountAtLeast(10)))
                .withAllowedLateness(Duration.ZERO)
                .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
                .accumulatingFiredPanes());

    PCollection<KV<Long, Iterable<Long>>> sessions =
        windowed.apply(GroupByKey.<Long, Long>create());

    PCollection<KV<Long, Iterable<Long>>> rewindowed =
        sessions.apply(
            Window.<KV<Long, Iterable<Long>>>into(FixedWindows.of(Duration.standardMinutes(1L))));

    rewindowed.apply(
        ParDo.of(
            new DoFn<KV<Long, Iterable<Long>>, Void>() {
              @ProcessElement
              public void showAllWindowsStartAtEpoch(ProcessContext ctxt, IntervalWindow window) {
                // Will always throw eventually if speculative panes can be produced
                checkArgument(
                    window.start().equals(new Instant(0)),
                    "Windows should only start as the epoch, as all possible sessions do. Got a %s",
                    window);
              }
            }));

    p.run().waitUntilFinish();
  }
}
