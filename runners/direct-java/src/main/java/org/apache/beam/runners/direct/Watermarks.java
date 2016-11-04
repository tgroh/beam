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

import static org.apache.beam.runners.direct.Watermark.THE_END_OF_TIME;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/** A class containing types of Watermarks. */
final class Watermarks {
  private Watermarks() {}
  private static final Ordering<Instant> INSTANT_ORDERING = Ordering.natural();

  /**
   * The input {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>At any point, the value of an {@link AppliedPTransformInputWatermark} is equal to the
   * minimum watermark across all of its input {@link Watermark Watermarks}, and the minimum
   * timestamp of all of the pending elements, restricted to be monotonically increasing.
   *
   * <p>See {@link #refresh()} for more information.
   */
  static class AppliedPTransformInputWatermark implements Watermark {
    private final Collection<? extends Watermark> inputWatermarks;
    private final SortedSet<CommittedBundle<?>> pendingElements;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> objectTimers;

    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformInputWatermark(Collection<? extends Watermark> inputWatermarks) {
      this.inputWatermarks = inputWatermarks;
      // The ordering must order elements by timestamp, and must not compare two distinct elements
      // as equal. This is built on the assumption that any element added as a pending element will
      // be consumed without modifications.
      Ordering<CommittedBundle<?>> pendingBundleComparator =
          new BundleByElementTimestampComparator().compound(Ordering.arbitrary());
      this.pendingElements =
          new TreeSet<>(pendingBundleComparator);
      this.objectTimers = new HashMap<>();
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformInputWatermark} becomes
     * equal to the maximum value of
     * <ul>
     *   <li>the previous input watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the timestamps of all currently pending elements</li>
     *       <li>all input {@link PCollection} watermarks</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public Update refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant minInputWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Watermark inputWatermark : inputWatermarks) {
        minInputWatermark = INSTANT_ORDERING.min(minInputWatermark, inputWatermark.get());
      }
      if (!pendingElements.isEmpty()) {
        minInputWatermark =
            INSTANT_ORDERING.min(minInputWatermark, pendingElements.first().getMinTimestamp());
      }
      Instant newWatermark = INSTANT_ORDERING.max(oldWatermark, minInputWatermark);
      currentWatermark.set(newWatermark);
      return Update.fromTimestamps(oldWatermark, newWatermark);
    }

    private void addPendingElements(CommittedBundle<?> newPending) {
      pendingElements.add(newPending);
    }

    private void removePendingElements(CommittedBundle<?> finishedElements) {
      pendingElements.remove(finishedElements);
    }

    private void updateTimers(TimerUpdate update) {
      NavigableSet<TimerData> keyTimers = objectTimers.get(update.getKey());
      if (keyTimers == null) {
        keyTimers = new TreeSet<>();
        objectTimers.put(update.getKey(), keyTimers);
      }
      for (TimerData timer : update.getSetTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          keyTimers.add(timer);
        }
      }
      for (TimerData timer : update.getDeletedTimers()) {
        if (TimeDomain.EVENT_TIME.equals(timer.getDomain())) {
          keyTimers.remove(timer);
        }
      }
      // We don't keep references to timers that have been fired and delivered via #getFiredTimers()
    }

    private Map<StructuralKey<?>, List<TimerData>> extractFiredEventTimeTimers() {
      return extractFiredTimers(currentWatermark.get(), objectTimers);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformInputWatermark.class)
          .add("pendingElements", pendingElements)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * The output {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>The value of an {@link AppliedPTransformOutputWatermark} is equal to the minimum of the
   * current watermark hold and the {@link AppliedPTransformInputWatermark} for the same
   * {@link AppliedPTransform}, restricted to be monotonically increasing. See
   * {@link #refresh()} for more information.
   */
  private static class AppliedPTransformOutputWatermark implements Watermark {
    private final Watermark inputWatermark;
    private final PerKeyHolds holds;
    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformOutputWatermark(AppliedPTransformInputWatermark inputWatermark) {
      this.inputWatermark = inputWatermark;
      holds = new PerKeyHolds();
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    public void updateHold(Object key, Instant newHold) {
      if (newHold == null) {
        holds.removeHold(key);
      } else {
        holds.updateHold(key, newHold);
      }
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformOutputWatermark} becomes
     * equal to the maximum value of:
     * <ul>
     *   <li>the previous output watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the current input watermark</li>
     *       <li>the current watermark holds</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public Update refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant newWatermark = INSTANT_ORDERING.min(inputWatermark.get(), holds.getMinHold());
      newWatermark = INSTANT_ORDERING.max(oldWatermark, newWatermark);
      currentWatermark.set(newWatermark);
      return Update.fromTimestamps(oldWatermark, newWatermark);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformOutputWatermark.class)
          .add("holds", holds)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * The input {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an
   * {@link AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeInputWatermark} is equal
   * to the minimum across all pending bundles at the {@link AppliedPTransform} and all upstream
   * {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the input
   * synchronized processing time at any step is equal to the maximum of:
   * <ul>
   *   <li>The most recently returned synchronized processing input time
   *   <li>The minimum of
   *     <ul>
   *       <li>The current processing time
   *       <li>The current synchronized processing time input hold
   *     </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeInputWatermark implements Watermark {
    private final Collection<? extends Watermark> inputWms;
    private final Collection<CommittedBundle<?>> pendingBundles;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> processingTimers;
    private final Map<StructuralKey<?>, NavigableSet<TimerData>> synchronizedProcessingTimers;

    private final NavigableSet<TimerData> pendingTimers;

    private AtomicReference<Instant> earliestHold;

    public SynchronizedProcessingTimeInputWatermark(Collection<? extends Watermark> inputWms) {
      this.inputWms = inputWms;
      this.pendingBundles = new HashSet<>();
      this.processingTimers = new HashMap<>();
      this.synchronizedProcessingTimers = new HashMap<>();
      this.pendingTimers = new TreeSet<>();
      Instant initialHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Watermark wm : inputWms) {
        initialHold = INSTANT_ORDERING.min(initialHold, wm.get());
      }
      earliestHold = new AtomicReference<>(initialHold);
    }

    @Override
    public Instant get() {
      return earliestHold.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link SynchronizedProcessingTimeInputWatermark}
     * becomes equal to the minimum value of
     * <ul>
     *   <li>the timestamps of all currently pending bundles</li>
     *   <li>all input {@link PCollection} synchronized processing time watermarks</li>
     * </ul>
     *
     * <p>Note that this value is not monotonic, but the returned value for the synchronized
     * processing time must be.
     */
    @Override
    public Update refresh() {
      Instant oldHold = earliestHold.get();
      Instant minTime = THE_END_OF_TIME.get();
      for (Watermark input : inputWms) {
        minTime = INSTANT_ORDERING.min(minTime, input.get());
      }
      for (CommittedBundle<?> bundle : pendingBundles) {
        // TODO: Track elements in the bundle by the processing time they were output instead of
        // entire bundles. Requried to support arbitrarily splitting and merging bundles between
        // steps
        minTime = INSTANT_ORDERING.min(minTime, bundle.getSynchronizedProcessingOutputWatermark());
      }
      earliestHold.set(minTime);
      return Update.fromTimestamps(oldHold, minTime);
    }

    public void addPending(CommittedBundle<?> bundle) {
      pendingBundles.add(bundle);
    }

    public void removePending(CommittedBundle<?> bundle) {
      pendingBundles.remove(bundle);
    }

    /**
     * Return the earliest timestamp of the earliest timer that has not been completed. This is
     * either the earliest timestamp across timers that have not been completed, or the earliest
     * timestamp across timers that have been delivered but have not been completed.
     */
    public Instant getEarliestTimerTimestamp() {
      Instant earliest = THE_END_OF_TIME.get();
      for (NavigableSet<TimerData> timers : processingTimers.values()) {
        if (!timers.isEmpty()) {
          earliest = INSTANT_ORDERING.min(timers.first().getTimestamp(), earliest);
        }
      }
      for (NavigableSet<TimerData> timers : synchronizedProcessingTimers.values()) {
        if (!timers.isEmpty()) {
          earliest = INSTANT_ORDERING.min(timers.first().getTimestamp(), earliest);
        }
      }
      if (!pendingTimers.isEmpty()) {
        earliest = INSTANT_ORDERING.min(pendingTimers.first().getTimestamp(), earliest);
      }
      return earliest;
    }

    private void updateTimers(TimerUpdate update) {
      Map<TimeDomain, NavigableSet<TimerData>> timerMap = timerMap(update.key);
      for (TimerData addedTimer : update.setTimers) {
        NavigableSet<TimerData> timerQueue = timerMap.get(addedTimer.getDomain());
        if (timerQueue != null) {
          timerQueue.add(addedTimer);
        }
      }

      for (TimerData completedTimer : update.completedTimers) {
        pendingTimers.remove(completedTimer);
      }
      for (TimerData deletedTimer : update.deletedTimers) {
        NavigableSet<TimerData> timerQueue = timerMap.get(deletedTimer.getDomain());
        if (timerQueue != null) {
          timerQueue.remove(deletedTimer);
        }
      }
    }

    private Map<StructuralKey<?>, List<TimerData>> extractFiredDomainTimers(
        TimeDomain domain, Instant firingTime) {
      Map<StructuralKey<?>, List<TimerData>> firedTimers;
      switch (domain) {
        case PROCESSING_TIME:
          firedTimers = extractFiredTimers(firingTime, processingTimers);
          break;
        case SYNCHRONIZED_PROCESSING_TIME:
          firedTimers =
              extractFiredTimers(
                  INSTANT_ORDERING.min(firingTime, earliestHold.get()),
                  synchronizedProcessingTimers);
          break;
        default:
          throw new IllegalArgumentException(
              "Called getFiredTimers on a Synchronized Processing Time watermark"
                  + " and gave a non-processing time domain "
                  + domain);
      }
      for (Map.Entry<StructuralKey<?>, ? extends Collection<TimerData>> firedTimer :
          firedTimers.entrySet()) {
        pendingTimers.addAll(firedTimer.getValue());
      }
      return firedTimers;
    }

    private Map<TimeDomain, NavigableSet<TimerData>> timerMap(StructuralKey<?> key) {
      NavigableSet<TimerData> processingQueue = processingTimers.get(key);
      if (processingQueue == null) {
        processingQueue = new TreeSet<>();
        processingTimers.put(key, processingQueue);
      }
      NavigableSet<TimerData> synchronizedProcessingQueue =
          synchronizedProcessingTimers.get(key);
      if (synchronizedProcessingQueue == null) {
        synchronizedProcessingQueue = new TreeSet<>();
        synchronizedProcessingTimers.put(key, synchronizedProcessingQueue);
      }
      EnumMap<TimeDomain, NavigableSet<TimerData>> result = new EnumMap<>(TimeDomain.class);
      result.put(TimeDomain.PROCESSING_TIME, processingQueue);
      result.put(TimeDomain.SYNCHRONIZED_PROCESSING_TIME, synchronizedProcessingQueue);
      return result;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(SynchronizedProcessingTimeInputWatermark.class)
          .add("earliestHold", earliestHold)
          .toString();
    }
  }

  /**
   * The output {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} hold for an
   * {@link AppliedPTransform}.
   *
   * <p>At any point, the hold value of an {@link SynchronizedProcessingTimeOutputWatermark} is
   * equal to the minimum across all incomplete timers at the {@link AppliedPTransform} and all
   * upstream {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} watermarks. The value of the output
   * synchronized processing time at any step is equal to the maximum of:
   * <ul>
   *   <li>The most recently returned synchronized processing output time
   *   <li>The minimum of
   *     <ul>
   *       <li>The current processing time
   *       <li>The current synchronized processing time output hold
   *     </ul>
   * </ul>
   */
  private static class SynchronizedProcessingTimeOutputWatermark implements Watermark {
    private final SynchronizedProcessingTimeInputWatermark inputWm;
    private AtomicReference<Instant> latestRefresh;

    public SynchronizedProcessingTimeOutputWatermark(
        SynchronizedProcessingTimeInputWatermark inputWm) {
      this.inputWm = inputWm;
      this.latestRefresh = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant get() {
      return latestRefresh.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link SynchronizedProcessingTimeOutputWatermark}
     * becomes equal to the minimum value of:
     * <ul>
     *   <li>the current input watermark.
     *   <li>all {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} timers that are based on the input
     *       watermark.
     *   <li>all {@link TimeDomain#PROCESSING_TIME} timers that are based on the input watermark.
     * </ul>
     *
     * <p>Note that this value is not monotonic, but the returned value for the synchronized
     * processing time must be.
     */
    @Override
    public Update refresh() {
      // Hold the output synchronized processing time to the input watermark, which takes into
      // account buffered bundles, and the earliest pending timer, which determines what to hold
      // downstream timers to.
      Instant oldRefresh = latestRefresh.get();
      Instant newTimestamp =
          INSTANT_ORDERING.min(inputWm.get(), inputWm.getEarliestTimerTimestamp());
      latestRefresh.set(newTimestamp);
      return Update.fromTimestamps(oldRefresh, newTimestamp);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(SynchronizedProcessingTimeOutputWatermark.class)
          .add("latestRefresh", latestRefresh)
          .toString();
    }
  }

  private static class BundleByElementTimestampComparator extends Ordering<CommittedBundle<?>> {
    @Override
    public int compare(CommittedBundle<?> o1, CommittedBundle<?> o2) {
      return ComparisonChain.start()
          .compare(o1.getMinTimestamp(), o2.getMinTimestamp())
          .result();
    }
  }

  private static class PerKeyHolds {
    private final Map<Object, KeyedHold> keyedHolds;
    private final NavigableSet<KeyedHold> allHolds;

    private PerKeyHolds() {
      this.keyedHolds = new HashMap<>();
      this.allHolds = new TreeSet<>();
    }

    /**
     * Gets the minimum hold across all keys in this {@link PerKeyHolds}, or THE_END_OF_TIME if
     * there are no holds within this {@link PerKeyHolds}.
     */
    public Instant getMinHold() {
      return allHolds.isEmpty() ? THE_END_OF_TIME.get() : allHolds.first().getTimestamp();
    }

    /**
     * Updates the hold of the provided key to the provided value, removing any other holds for
     * the same key.
     */
    public void updateHold(@Nullable Object key, Instant newHold) {
      removeHold(key);
      KeyedHold newKeyedHold = KeyedHold.of(key, newHold);
      keyedHolds.put(key, newKeyedHold);
      allHolds.add(newKeyedHold);
    }

    /**
     * Removes the hold of the provided key.
     */
    public void removeHold(Object key) {
      KeyedHold oldHold = keyedHolds.remove(key);
      if (oldHold != null) {
        allHolds.remove(oldHold);
      }
    }
  }

  /**
   * A (key, Instant) pair that holds the watermark. Holds are per-key, but the watermark is global,
   * and as such the watermark manager must track holds and the release of holds on a per-key basis.
   *
   * <p>The {@link #compareTo(KeyedHold)} method of {@link KeyedHold} is not consistent with equals,
   * as the key is arbitrarily ordered via identity, rather than object equality.
   */
  private static final class KeyedHold implements Comparable<KeyedHold> {
    private static final Ordering<Object> KEY_ORDERING = Ordering.arbitrary().nullsLast();

    private final Object key;
    private final Instant timestamp;

    /**
     * Create a new KeyedHold with the specified key and timestamp.
     */
    public static KeyedHold of(Object key, Instant timestamp) {
      return new KeyedHold(key, MoreObjects.firstNonNull(timestamp, THE_END_OF_TIME.get()));
    }

    private KeyedHold(Object key, Instant timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }

    @Override
    public int compareTo(KeyedHold that) {
      return ComparisonChain.start()
          .compare(this.timestamp, that.timestamp)
          .compare(this.key, that.key, KEY_ORDERING)
          .result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, key);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof KeyedHold)) {
        return false;
      }
      KeyedHold that = (KeyedHold) other;
      return Objects.equals(this.timestamp, that.timestamp) && Objects.equals(this.key, that.key);
    }

    /**
     * Get the value of this {@link KeyedHold}.
     */
    public Instant getTimestamp() {
      return timestamp;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(KeyedHold.class)
          .add("key", key)
          .add("hold", timestamp)
          .toString();
    }
  }

  /**
   * For each (Object, NavigableSet) pair in the provided map, remove each Timer that is before the
   * latestTime argument and put in in the result with the same key, then remove all of the keys
   * which have no more pending timers.
   *
   * <p>The result collection retains ordering of timers (from earliest to latest).
   */
  private static Map<StructuralKey<?>, List<TimerData>> extractFiredTimers(
      Instant latestTime, Map<StructuralKey<?>, NavigableSet<TimerData>> objectTimers) {
    Map<StructuralKey<?>, List<TimerData>> result = new HashMap<>();
    Set<StructuralKey<?>> emptyKeys = new HashSet<>();
    for (Map.Entry<StructuralKey<?>, NavigableSet<TimerData>> pendingTimers :
        objectTimers.entrySet()) {
      NavigableSet<TimerData> timers = pendingTimers.getValue();
      if (!timers.isEmpty() && timers.first().getTimestamp().isBefore(latestTime)) {
        ArrayList<TimerData> keyFiredTimers = new ArrayList<>();
        result.put(pendingTimers.getKey(), keyFiredTimers);
        while (!timers.isEmpty() && timers.first().getTimestamp().isBefore(latestTime)) {
          keyFiredTimers.add(timers.first());
          timers.remove(timers.first());
        }
      }
      if (timers.isEmpty()) {
        emptyKeys.add(pendingTimers.getKey());
      }
    }
    objectTimers.keySet().removeAll(emptyKeys);
    return result;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

}
