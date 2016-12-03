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
 *
 */

package org.apache.beam.sdk.values;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PCollection}.
 */
@RunWith(JUnit4.class)
public class PCollectionTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private TestPipeline p = TestPipeline.create();

  @Test
  public void compatibleCollectionsVerifyCompatible() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    theirs.finishSpecifying();

    ours.verifyCompatible(theirs);
  }

  @Test
  public void compatibleCollectionsCustomWindowingAreCompatible() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p,
                WindowingStrategy.globalDefault()
                    .withWindowFn(
                        SlidingWindows.of(Duration.standardMinutes(1))
                            .every(Duration.standardSeconds(15)))
                    .withTrigger(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(10_000))),
                IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p,
                WindowingStrategy.globalDefault()
                    .withWindowFn(
                        SlidingWindows.of(Duration.standardMinutes(1))
                            .every(Duration.standardSeconds(15)))
                    .withTrigger(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(10_000))),
                IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    theirs.finishSpecifying();

    ours.verifyCompatible(theirs);
  }

  @Test
  public void notFinishedSpecifyingVerifyCompatibleFails() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must be finished specifying");
    thrown.expectMessage(ours.toString());
    ours.verifyCompatible(ours);
  }

  @Test
  public void thiersNotFinishedSpecifyingVerifyCompatibleFails() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    ours.finishSpecifying();

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must be finished specifying");
    thrown.expectMessage(theirs.toString());
    ours.verifyCompatible(theirs);
  }

  @Test
  public void differentCodersNotCompatible() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(BigEndianIntegerCoder.of());
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
            p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    theirs.finishSpecifying();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Coder mismatch");
    thrown.expectMessage(VarIntCoder.of().toString());
    thrown.expectMessage(BigEndianIntegerCoder.of().toString());
    ours.verifyCompatible(theirs);
  }

  @Test
  public void differentBoundednessNotCompatible() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(VarIntCoder.of());
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    theirs.finishSpecifying();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("IsBounded mismatch");
    thrown.expectMessage(IsBounded.BOUNDED.toString());
    thrown.expectMessage(IsBounded.UNBOUNDED.toString());
    ours.verifyCompatible(theirs);
  }

  @Test
  public void differentWindowingStrategiesNotCompatible() {
    WindowingStrategy<Object, GlobalWindow> ourWindowingStrategy =
        WindowingStrategy.globalDefault();
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, ourWindowingStrategy, IsBounded.UNBOUNDED)
            .setCoder(VarIntCoder.of());
    WindowingStrategy<Object, GlobalWindow> theirWindowingStrategy =
        WindowingStrategy.globalDefault()
            .withAllowedLateness(Duration.standardMinutes(20))
            .withTrigger(AfterPane.elementCountAtLeast(1));
    PCollection<Integer> theirs =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, theirWindowingStrategy, IsBounded.UNBOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    theirs.finishSpecifying();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("WindowingStrategy mismatch");
    thrown.expectMessage(theirWindowingStrategy.toString());
    thrown.expectMessage(ourWindowingStrategy.toString());
    ours.verifyCompatible(theirs);
  }

  @Test
  public void notAPCollectionNotCompatible() {
    PCollection<Integer> ours =
        PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(VarIntCoder.of());
    ours.finishSpecifying();
    PCollectionView<Integer> theirs =
        PCollectionViews.singletonView(
            p, WindowingStrategy.globalDefault(), false, null, VarIntCoder.of());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        String.format("%s is not a %s", theirs, PCollection.class.getSimpleName()));
    ours.verifyCompatible(theirs);
  }
}
