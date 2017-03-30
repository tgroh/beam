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

package org.apache.beam.examples.cookbook;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.PubsubIO.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer and Reader to pubsub.
 */
public class DoublePubsubExample implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DoublePubsubExample.class);
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    Pipeline writer = Pipeline.create(options);
    writer
        .apply(CountingInput.unbounded().withRate(10, Duration.standardSeconds(1L)))
        .apply(Window.<Long>into(FixedWindows.of(Duration.standardMinutes(1L))))
        .apply(
            ParDo.of(
                new DoFn<Long, TimestampedValue<Long>>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    context.output(
                        TimestampedValue.of(
                            context.element() + ThreadLocalRandom.current().nextLong(-10, 10),
                            context.timestamp()));
                  }
                }))
        .apply(
            PubsubIO.<TimestampedValue<Long>>write()
                .topic("$TARGET_TOPIC")
                .idLabel("id")
                .timestampLabel("ts")
                .withAttributes(
                    new SimpleFunction<TimestampedValue<Long>, PubsubMessage>() {
                      @Override
                      public PubsubMessage apply(TimestampedValue<Long> input) {
                        Map<String, String> attributes =
                            ImmutableMap.<String, String>builder()
                                .put("id", Long.toHexString(input.getValue() % 256))
                                .put(
                                    "ts", DateTimeFormat.fullDateTime().print(input.getTimestamp()))
                                .build();
                        if (input.getValue() % 256 == 0) {
                          LOG.info("Writing {} to Pubsub", input);
                        }
                        try {
                          return new PubsubMessage(
                              CoderUtils.encodeToByteArray(VarLongCoder.of(), input.getValue()),
                              attributes);
                        } catch (CoderException e) {
                          throw new IllegalStateException(e);
                        }
                      }
                    }));
    Pipeline reader = Pipeline.create(options);
    reader
        .apply(
            PubsubIO.<KV<String, Long>>read()
                .subscription("$TARGET_SUB")
                .timestampLabel("ts")
                .idLabel("id")
                .withAttributes(
                    new SimpleFunction<PubsubMessage, KV<String, Long>>() {
                      @Override
                      public KV<String, Long> apply(PubsubMessage input) {
                        try {
                          return KV.of(
                              input.getAttribute("id"),
                              CoderUtils.decodeFromByteArray(
                                  VarLongCoder.of(), input.getMessage()));
                        } catch (CoderException e) {
                          throw new IllegalArgumentException(e);
                        }
                      }
                    })
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
        .apply(ParDo.of(new DoFn<KV<String, Long>, KV<String, Long>>() {
          @ProcessElement
          public void printSometimes(ProcessContext context) {
            if (context.element().getValue() % 64 == 0) {
              LOG.info("Got element {} at Timestamp {}", context.element(), context.timestamp());
            }
          }
        }));

    PipelineResult writerResult = writer.run();
    System.out.println("Started Writer");
    PipelineResult readerResult = reader.run();
    LOG.info("Started Reader");
    System.out.println("Started Reader");
    readerResult.waitUntilFinish();
  }
}
