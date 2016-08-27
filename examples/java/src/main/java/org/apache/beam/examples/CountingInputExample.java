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

package org.apache.beam.examples;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max.MaxLongFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * An example of CountingInput.
 */
public class CountingInputExample implements Serializable {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline p = Pipeline.create(options);
    PCollection<KV<Long, Long>> myLongs =
        p.apply("QuickCount", CountingInput.unbounded())
            .apply("AFunction", ParDo.of(new IdentityFn<Long>()))
            .apply(
                "TriggerOften",
                Window.<Long>triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(10000)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(
                "KeyWithSelf",
                WithKeys.of(
                    new SerializableFunction<Long, Long>() {
                      @Override
                      public Long apply(Long input) {
                        return input;
                      }
                    }))
            .apply("ForceFusionBreak", Reshuffle.<Long, Long>of())
            .apply("ASecondFunction", ParDo.of(new IdentityFn<KV<Long, Long>>()));

    myLongs.apply("AggregatorOfLargest", ParDo.of(new DoFn<KV<Long, Long>, Long>() {
      Aggregator<Long, Long> countAgg = createAggregator("LargestElement", new MaxLongFn());

      @ProcessElement
      public void addCounter(ProcessContext c) {
        countAgg.addValue(c.element().getKey());
      }
    }));

    PCollection<Long> count =
        myLongs.apply("Unkey", Values.<Long>create()).apply("TotalCount", Count.<Long>globally());
    count.apply("AddToCount", ParDo.of(new DoFn<Long, Long>() {
      Aggregator<Long, Long> countAgg = createAggregator("TotalElements", new Sum.SumLongFn());

      @ProcessElement
      public void addCounter(ProcessContext c) {
        countAgg.addValue(c.element());
      }
    }));

    p.run();
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    int field = 3;
    long otherField = Integer.MAX_VALUE + 1L;
    String myString = "foobar";
    byte[] alsoExists = new byte[] {1, 2, 4, 8, 16, 32, 64, -128, -64, -32, -16, -8, -4, -2, -1, 0};

    boolean firstCall = false;
    Aggregator<Long, Long> numInstances =
        createAggregator("TotalInstancesDeserialized", new Sum.SumLongFn());

    @StartBundle
    public void setup(Context c) {
      if (!firstCall) {
        firstCall = true;
        numInstances.addValue(1L);
      }
    }

    @ProcessElement
    public void outputInput(ProcessContext c) {
      c.output(c.element());
    }
  }
}
