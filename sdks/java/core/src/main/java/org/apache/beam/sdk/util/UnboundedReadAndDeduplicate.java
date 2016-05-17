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

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;

import org.joda.time.Duration;

import autovalue.shaded.com.google.common.common.collect.Iterables;

/**
 * Foo.
 */
public class UnboundedReadAndDeduplicate<T> extends PTransform<PBegin, PCollection<T>> {
  public static <T> UnboundedReadAndDeduplicate<T> from(Read.Unbounded<T> original) {
    return new UnboundedReadAndDeduplicate<>(original);
  }

  private final Read.Unbounded<T> original;

  private UnboundedReadAndDeduplicate(Unbounded<T> original) {
    this.original = original;
  }

  @Override
  public PCollection<T> apply(PBegin input) {
    PCollection<KV<byte[], T>> records = input.apply(ReadWithIds.of(original.getSource()));
    if (original.getSource().requiresDeduping()) {
      PCollection<KV<byte[], T>> dedupeSessions =
          records.apply(Window.<KV<byte[], T>>into(
                                  Sessions.withGapDuration(Duration.standardMinutes(10L)))
                              .triggering(AfterPane.elementCountAtLeast(1))
                              .accumulatingFiredPanes());
      PCollection<KV<byte[], Iterable<T>>> deduplicatedRecords =
          dedupeSessions.apply(Sample.<byte[], T>fixedSizePerKey(1));
      // restore the original windowing strategy)
      PCollection<T> actuals = deduplicatedRecords.apply(ParDo.of(new GetOnlyRecordFn<T>()));
      // Rewindow back into the global window, then reset the original windowing strategy
      return actuals.apply(Window.<T>into(new GlobalWindows()))
          .setWindowingStrategyInternal(records.getWindowingStrategy());
    } else {
      // No need to deduplicate, just output the records
      return records.apply(Values.<T>create());
    }
  }

  /**
   * A primitive {@link PTransform} that reads from an unbounded source, producing (Record,
   * RecordID) pairs to allow the runner to deduplicate the inputs.
   */
  public static final class ReadWithIds<T>
      extends PTransform<PBegin, PCollection<KV<byte[], T>>> {
    public static <T> ReadWithIds<T> of(UnboundedSource<T, ?> source) {
      return new ReadWithIds<>(source);
    }

    private final UnboundedSource<T, ?> source;

    public ReadWithIds(UnboundedSource<T, ?> source) {
      this.source = source;
    }

    @Override
    public PCollection<KV<byte[], T>> apply(PBegin input) {
      return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
          WindowingStrategy.globalDefault(),
          IsBounded.UNBOUNDED);
    }

    public UnboundedSource<T, ?> getSource() {
      return source;
    }
  }

  private static class GetOnlyRecordFn<T> extends DoFn<KV<byte[], Iterable<T>>, T> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(Iterables.getOnlyElement(c.element().getValue()));
    }
  }
}
