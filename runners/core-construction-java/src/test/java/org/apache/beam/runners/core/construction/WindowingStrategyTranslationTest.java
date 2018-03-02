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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.AccumulationMode.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MergeStatus;
import org.apache.beam.model.pipeline.v1.RunnerApi.OutputTime;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger.Never;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link WindowingStrategy}. */
@RunWith(Enclosed.class)
public class WindowingStrategyTranslationTest {
  // Each spec activates tests of all subsets of its fields
  @AutoValue
  abstract static class ToProtoAndBackSpec {
    abstract WindowingStrategy getWindowingStrategy();
  }

  /**
   * Tests for {@link WindowingStrategyTranslation} where the {@link WindowingStrategy windowing
   * strategies} are constructed in a Java SDK.
   */
  @RunWith(Parameterized.class)
  public static class JavaStrategyTranslationTest {
    private static ToProtoAndBackSpec toProtoAndBackSpec(WindowingStrategy windowingStrategy) {
      return new AutoValue_WindowingStrategyTranslationTest_ToProtoAndBackSpec(windowingStrategy);
    }

    private static final WindowFn<?, ?> REPRESENTATIVE_WINDOW_FN =
        FixedWindows.of(Duration.millis(12));

    private static final Trigger REPRESENTATIVE_TRIGGER = AfterWatermark.pastEndOfWindow();

    @Parameters(name = "{index}: {0}")
    public static Iterable<ToProtoAndBackSpec> data() {
      return ImmutableList.of(
          toProtoAndBackSpec(WindowingStrategy.globalDefault()),
          toProtoAndBackSpec(
              WindowingStrategy.of(
                  FixedWindows.of(Duration.millis(11)).withOffset(Duration.millis(3)))),
          toProtoAndBackSpec(
              WindowingStrategy.of(
                  SlidingWindows.of(Duration.millis(37))
                      .every(Duration.millis(3))
                      .withOffset(Duration.millis(2)))),
          toProtoAndBackSpec(WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(389)))),
          toProtoAndBackSpec(
              WindowingStrategy.of(REPRESENTATIVE_WINDOW_FN)
                  .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS)
                  .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
                  .withTrigger(REPRESENTATIVE_TRIGGER)
                  .withAllowedLateness(Duration.millis(71))
                  .withTimestampCombiner(TimestampCombiner.EARLIEST)),
          toProtoAndBackSpec(
              WindowingStrategy.of(REPRESENTATIVE_WINDOW_FN)
                  .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY)
                  .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
                  .withTrigger(REPRESENTATIVE_TRIGGER)
                  .withAllowedLateness(Duration.millis(93))
                  .withTimestampCombiner(TimestampCombiner.LATEST)));
    }

    @Parameter(0)
    public ToProtoAndBackSpec toProtoAndBackSpec;

    @Test
    public void testToProtoAndBack() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
      WindowingStrategy<?, ?> toProtoAndBackWindowingStrategy =
          WindowingStrategyTranslation.fromProto(
              WindowingStrategyTranslation.toProto(windowingStrategy));

      assertThat(
          toProtoAndBackWindowingStrategy,
          equalTo((WindowingStrategy) windowingStrategy.fixDefaults()));
    }

    @Test
    public void testToProtoAndBackWithComponents() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
      SdkComponents components = SdkComponents.create();
      RunnerApi.WindowingStrategy proto =
          WindowingStrategyTranslation.toProto(windowingStrategy, components);
      RehydratedComponents protoComponents =
          RehydratedComponents.forComponents(components.toComponents());

      assertThat(
          WindowingStrategyTranslation.fromProto(proto, protoComponents).fixDefaults(),
          Matchers.equalTo(windowingStrategy.fixDefaults()));

      protoComponents.getCoder(
          components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));
      assertThat(
          proto.getAssignsToOneWindow(),
          equalTo(windowingStrategy.getWindowFn().assignsToOneWindow()));
    }
  }

  /**
   * Tests for {@link WindowingStrategyTranslation} where the {@link WindowingStrategy windowing
   * strategies} are provided in a portable (non-java-compatible) form.
   */
  @RunWith(JUnit4.class)
  public static class PortableWindowingStrategyTest {
    @Test
    public void unknownWindowFn() throws Exception {
      SdkFunctionSpec windowSpec =
          SdkFunctionSpec.newBuilder()
              .setEnvironmentId("py")
              .setSpec(
                  FunctionSpec.newBuilder()
                      .setUrn("beam:fn:window:py:v1")
                      .setPayload(ByteString.copyFrom(new byte[] {0, 1, 2, 3, -128, 2, 5, -11}))
                      .build())
              .build();
      RunnerApi.WindowingStrategy strategy =
          RunnerApi.WindowingStrategy.newBuilder()
              .setWindowCoderId("window_coder")
              .setAssignsToOneWindow(false)
              .setAllowedLateness(1000L)
              .setTrigger(
                  RunnerApi.Trigger.newBuilder().setNever(Never.getDefaultInstance()).build())
              .setWindowFn(windowSpec)
              .setAccumulationMode(Enum.ACCUMULATING)
              .setClosingBehavior(RunnerApi.ClosingBehavior.Enum.EMIT_ALWAYS)
              .setMergeStatus(MergeStatus.Enum.NON_MERGING)
              .setOutputTime(OutputTime.Enum.LATEST_IN_PANE)
              .build();
      Components components =
          Components.newBuilder()
              .putWindowingStrategies("strategy", strategy)
              .putCoders(
                  "window_coder",
                  Coder.newBuilder()
                      .setSpec(
                          SdkFunctionSpec.newBuilder()
                              .setEnvironmentId("py")
                              .setSpec(
                                  FunctionSpec.newBuilder()
                                      .setUrn("beam:coder:py:custom_window:v1")
                                      .setPayload(
                                          ByteString.copyFrom(new byte[] {12, 24, 44, -14}))))
                      .build())
              .build();

      WindowingStrategy<?, ?> rehydrated =
          WindowingStrategyTranslation.fromProto(
              strategy, RehydratedComponents.forComponents(components));
      assertThat(rehydrated.getWindowFn(), not(nullValue()));
    }
  }
}
