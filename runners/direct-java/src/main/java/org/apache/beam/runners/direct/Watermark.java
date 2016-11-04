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

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * The watermark of some {@link Pipeline} element, usually a {@link PTransform} or a
 * {@link PCollection}.
 *
 * <p>A watermark is a monotonically increasing value, which represents the point up to which the
 * system believes it has received all of the data. Data that arrives with a timestamp that is
 * before the watermark is considered late. {@link BoundedWindow#TIMESTAMP_MAX_VALUE} is a special
 * timestamp which indicates we have received all of the data and there will be no more on-time or
 * late data. This value is represented by {@link WatermarkManager#THE_END_OF_TIME}.
 */
interface Watermark {
  /**
   * Returns the current value of this watermark.
   */
  Instant get();

  /**
   * Refreshes the value of this watermark from its input watermarks and watermark holds.
   *
   * @return true if the value of the watermark has changed (and thus dependent watermark must
   *         also be updated
   */
  Update refresh();

  void addPending(CommittedBundle<?> bundle);
  void removePending(CommittedBundle<?> bundle);

  Map<StructuralKey<?>, List<TimerData>> extractFiredTimers(
      TimeDomain eventTime, Instant reportedWm);

  /** The result of computing a {@link Watermark}. */
  enum Update {
    /** The watermark is later than the value at the previous time it was computed. */
    ADVANCED(true),
    /** The watermark is equal to the value at the previous time it was computed. */
    NO_CHANGE(false);

    private final boolean advanced;

    private Update(boolean advanced) {
      this.advanced = advanced;
    }

    public boolean isAdvanced() {
      return advanced;
    }

    /**
     * Returns the {@link Update} that is a result of combining the two watermark updates.
     *
     * <p>If either of the input {@link Update WatermarkUpdates} were advanced, the result {@link
     * Update} has been advanced.
     */
    public Update union(Update that) {
      if (this.advanced) {
        return this;
      }
      return that;
    }

    /** Returns the {@link Update} based on the former and current {@link Instant timestamps}. */
    public static Update fromTimestamps(Instant oldTime, Instant currentTime) {
      if (currentTime.isAfter(oldTime)) {
        return ADVANCED;
      }
      return NO_CHANGE;
    }
  }

  /**
   * The {@code Watermark} that is after the latest time it is possible to represent in the global
   * window. This is a distinguished value representing a complete {@link PTransform}.
   */
   Watermark THE_END_OF_TIME = new Watermark() {
    @Override
    public Update refresh() {
      // THE_END_OF_TIME is a distinguished value that cannot be advanced.
      return Update.NO_CHANGE;
    }

    @Override
    public void addPending(CommittedBundle<?> bundle) {
      throw new UnsupportedOperationException("addPending is not supported by THE_END_OF_TIME");
    }

    @Override
    public void removePending(CommittedBundle<?> bundle) {
      throw new UnsupportedOperationException("removePending is not supported by THE_END_OF_TIME");
    }

    @Override
    public Instant get() {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
  };

}
