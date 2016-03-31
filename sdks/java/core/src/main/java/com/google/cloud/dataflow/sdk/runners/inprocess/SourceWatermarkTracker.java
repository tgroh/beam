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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;

import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tracks the aggregate watermark across multiple splits of a {@link Source}.
 *
 * <p>This is used by {@link BoundedReadEvaluatorFactory} and {@link UnboundedReadEvaluatorFactory}
 * to ensure that no split advances the watermark past the split that is farthest behind. Failing
 * to do so would allow output to be produced before the source watermark believes that all input
 * is available.
 *
 * <p>This class is safe in the presence of concurrent updates.
 */
@ThreadSafe
class SourceWatermarkTracker {
  public static SourceWatermarkTracker forSources(Iterable<? extends Source<?>> shards) {
    return new SourceWatermarkTracker(shards);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  private final Map<Source<?>, Instant> watermarks;

  public SourceWatermarkTracker(Iterable<? extends Source<?>> shards) {
    watermarks = new HashMap<>();
    for (Source<?> shard : shards) {
      watermarks.put(shard, BoundedWindow.TIMESTAMP_MIN_VALUE);
    }
  }

  /**
   * Updates the watermark of the provided source to the new watermark.
   */
  public synchronized void setWatermark(Source<?> shard, Instant newWm) {
    watermarks.put(shard, newWm);
  }

  /**
   * Gets the watermark for all of the contained sources.
   */
  public synchronized Instant getWatermark() {
    Instant minWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Instant watermark : watermarks.values()) {
      if (watermark.isBefore(minWatermark)) {
        minWatermark = watermark;
      }
    }
    return minWatermark;
  }
}
