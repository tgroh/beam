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

package org.apache.beam.runners.local;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.local.TimerUpdate.TimerUpdateBuilder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TimerUpdate.TimerUpdateBuilder}. */
@RunWith(JUnit4.class)
public class TimerUpdateBuilderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void timerUpdateBuilderBuildAddsAllAddedTimers() {
    TimerData set = TimerData.of(StateNamespaces.global(), new Instant(10L), TimeDomain.EVENT_TIME);
    TimerData deleted =
        TimerData.of(StateNamespaces.global(), new Instant(24L), TimeDomain.PROCESSING_TIME);
    TimerData completedOne =
        TimerData.of(
            StateNamespaces.global(), new Instant(1024L), TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
    TimerData completedTwo =
        TimerData.of(StateNamespaces.global(), new Instant(2048L), TimeDomain.EVENT_TIME);

    TimerUpdate update =
        TimerUpdate.builder(StructuralKey.of("foo", StringUtf8Coder.of()))
            .withCompletedTimers(ImmutableList.of(completedOne, completedTwo))
            .setTimer(set)
            .deletedTimer(deleted)
            .build();

    assertThat(update.getCompletedTimers(), containsInAnyOrder(completedOne, completedTwo));
    assertThat(update.getSetTimers(), contains(set));
    assertThat(update.getDeletedTimers(), contains(deleted));
  }

  @Test
  public void timerUpdateBuilderWithSetAtEndOfTime() {
    Instant timerStamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    TimerData tooFar = TimerData.of(StateNamespaces.global(), timerStamp, TimeDomain.EVENT_TIME);

    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(timerStamp.toString());
    builder.setTimer(tooFar);
  }

  @Test
  public void timerUpdateBuilderWithSetPastEndOfTime() {
    Instant timerStamp = BoundedWindow.TIMESTAMP_MAX_VALUE.plus(Duration.standardMinutes(2));
    TimerData tooFar = TimerData.of(StateNamespaces.global(), timerStamp, TimeDomain.EVENT_TIME);

    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(timerStamp.toString());
    builder.setTimer(tooFar);
  }

  @Test
  public void timerUpdateBuilderWithSetThenDeleteHasOnlyDeleted() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.setTimer(timer).deletedTimer(timer).build();

    assertThat(built.getSetTimers(), emptyIterable());
    assertThat(built.getDeletedTimers(), contains(timer));
  }

  @Test
  public void timerUpdateBuilderWithDeleteThenSetHasOnlySet() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.deletedTimer(timer).setTimer(timer).build();

    assertThat(built.getSetTimers(), contains(timer));
    assertThat(built.getDeletedTimers(), emptyIterable());
  }

  @Test
  public void timerUpdateBuilderWithSetAfterBuildNotAddedToBuilt() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.build();
    builder.setTimer(timer);
    assertThat(built.getSetTimers(), emptyIterable());
    builder.build();
    assertThat(built.getSetTimers(), emptyIterable());
  }

  @Test
  public void timerUpdateBuilderWithDeleteAfterBuildNotAddedToBuilt() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.build();
    builder.deletedTimer(timer);
    assertThat(built.getDeletedTimers(), emptyIterable());
    builder.build();
    assertThat(built.getDeletedTimers(), emptyIterable());
  }

  @Test
  public void timerUpdateBuilderWithCompletedAfterBuildNotAddedToBuilt() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.build();
    builder.withCompletedTimers(ImmutableList.of(timer));
    assertThat(built.getCompletedTimers(), emptyIterable());
    builder.build();
    assertThat(built.getCompletedTimers(), emptyIterable());
  }

  @Test
  public void timerUpdateWithCompletedTimersNotAddedToExisting() {
    TimerUpdateBuilder builder = TimerUpdate.builder(StructuralKey.empty());
    TimerData timer = TimerData.of(StateNamespaces.global(), Instant.now(), TimeDomain.EVENT_TIME);

    TimerUpdate built = builder.build();
    assertThat(built.getCompletedTimers(), emptyIterable());
    assertThat(
        built.withCompletedTimers(ImmutableList.of(timer)).getCompletedTimers(), contains(timer));
    assertThat(built.getCompletedTimers(), emptyIterable());
  }
}
