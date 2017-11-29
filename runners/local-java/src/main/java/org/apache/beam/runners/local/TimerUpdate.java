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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * A collection of newly set, deleted, and completed timers.
 *
 * <p>setTimers and deletedTimers are collections of {@link TimerData} that have been added to the
 * {@code TimerInternals} of an executed step. completedTimers are timers that were delivered as
 * the input to the executed step.
 */
@AutoValue
public abstract class TimerUpdate {
  /**
   * Returns a TimerUpdate for a null key with no timers.
   */
  public static TimerUpdate empty() {
    return new AutoValue_TimerUpdate(
        StructuralKey.empty(),
        Collections.<TimerData>emptyList(),
        Collections.<TimerData>emptyList(),
        Collections.<TimerData>emptyList());
  }

  /**
   * Creates a new {@link TimerUpdate} builder with the provided completed timers that needs the
   * set and deleted timers to be added to it.
   */
  public static TimerUpdateBuilder builder(@Nonnull StructuralKey<?> key) {
    return new TimerUpdateBuilder(key);
  }

  /**
   * A {@link TimerUpdate} builder that needs to be provided with set timers and deleted timers.
   */
  public static final class TimerUpdateBuilder {
    private final StructuralKey<?> key;
    private final Collection<TimerData> completedTimers;
    private final Collection<TimerData> setTimers;
    private final Collection<TimerData> deletedTimers;

    private TimerUpdateBuilder(StructuralKey<?> key) {
      this.key = key;
      this.completedTimers = new HashSet<>();
      this.setTimers = new HashSet<>();
      this.deletedTimers = new HashSet<>();
    }

    /**
     * Adds all of the provided timers to the collection of completed timers, and returns this
     * {@link TimerUpdateBuilder}.
     */
    public TimerUpdateBuilder withCompletedTimers(Iterable<TimerData> completedTimers) {
      Iterables.addAll(this.completedTimers, completedTimers);
      return this;
    }

    /**
     * Adds the provided timer to the collection of set timers, removing it from deleted timers if
     * it has previously been deleted. Returns this {@link TimerUpdateBuilder}.
     */
    public TimerUpdateBuilder setTimer(TimerData setTimer) {
      checkArgument(
          setTimer.getTimestamp().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Got a timer for after the end of time (%s), got %s",
          BoundedWindow.TIMESTAMP_MAX_VALUE,
          setTimer.getTimestamp());
      deletedTimers.remove(setTimer);
      setTimers.add(setTimer);
      return this;
    }

    /**
     * Adds the provided timer to the collection of deleted timers, removing it from set timers if
     * it has previously been set. Returns this {@link TimerUpdateBuilder}.
     */
    public TimerUpdateBuilder deletedTimer(TimerData deletedTimer) {
      deletedTimers.add(deletedTimer);
      setTimers.remove(deletedTimer);
      return this;
    }

    /**
     * Returns a new {@link TimerUpdate} with the most recently set completedTimers, setTimers,
     * and deletedTimers.
     */
    public TimerUpdate build() {
      return new AutoValue_TimerUpdate(
          key,
          ImmutableSet.copyOf(completedTimers),
          ImmutableSet.copyOf(setTimers),
          ImmutableSet.copyOf(deletedTimers));
    }
  }

  public abstract StructuralKey<?> getKey();

  public abstract Iterable<? extends TimerData> getCompletedTimers();

  public abstract Iterable<? extends TimerData> getSetTimers();

  public abstract Iterable<? extends TimerData> getDeletedTimers();

  /**
   * Returns a {@link TimerUpdate} that is like this one, but with the specified completed timers.
   */
  public TimerUpdate withCompletedTimers(Iterable<TimerData> completedTimers) {
    return new AutoValue_TimerUpdate(getKey(), completedTimers, getSetTimers(), getDeletedTimers());
  }
}
