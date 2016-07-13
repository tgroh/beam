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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.Write.Bound;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.google.common.annotations.VisibleForTesting;

import org.joda.time.Duration;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Write} {@link PTransform PTransforms}
 * with an unspecified number of shards with a write with a specified number of shards. The number
 * of shards is uniformly randomly distributed between 3 and 7 shards.
 */
class WriteWithShardingFactory implements PTransformOverrideFactory {
  @Override
  public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
      PTransform<InputT, OutputT> transform) {
    if (transform instanceof Write.Bound) {
      Write.Bound<InputT> that = (Write.Bound<InputT>) transform;
      if (that.getNumShards() == 0) {
        return (PTransform<InputT, OutputT>) new DynamicallyReshardedWrite<InputT>(that);
      }
    }
    return transform;
  }

  private static class DynamicallyReshardedWrite <T> extends PTransform<PCollection<T>, PDone> {
    private final transient Write.Bound<T> original;

    private DynamicallyReshardedWrite(Bound<T> original) {
      this.original = original;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      PCollection<T> records = input.apply("RewindowInputs",
          Window.<T>into(new GlobalWindows()).triggering(DefaultTrigger.of())
              .withAllowedLateness(Duration.ZERO)
              .discardingFiredPanes());
      final PCollectionView<Long> numRecords = records.apply(Count.<T>globally().asSingletonView());
      PCollection<T> resharded =
          records
              .apply(
                  "ApplySharding",
                  ParDo.withSideInputs(numRecords)
                      .of(new KeyBasedOnCountFn<T>(numRecords)))
              .apply("GroupIntoShards", GroupByKey.<Long, T>create())
              .apply("DropShardingKeys", Values.<Iterable<T>>create())
              .apply("FlattenShardIterables", Flatten.<T>iterables());
      // This is an inverted application to apply the expansion of the original Write PTransform
      // without adding a new Write Transform Node, which would be overwritten the same way, leading
      // to an infinite recursion. We cannot modify the number of shards, because that is determined
      // at runtime.
      return original.apply(resharded);
    }
  }

  @VisibleForTesting
  static class KeyBasedOnCountFn<T> extends DoFn<T, KV<Long, T>> {
    private static final long MIN_SHARDS_FOR_LOG = 3L;
    private final PCollectionView<Long> numRecords;
    private long currentShard;
    private long maxShards;

    KeyBasedOnCountFn(PCollectionView<Long> numRecords) {
      this.numRecords = numRecords;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      if (maxShards == 0L) {
        maxShards = calculateShards(c.sideInput(numRecords));
        currentShard = ThreadLocalRandom.current().nextLong(maxShards);
      }
      long shard = currentShard;
      currentShard = (currentShard + 1) % maxShards;
      c.output(KV.of(shard, c.element()));
    }

    private long calculateShards(long totalRecords) {
      checkArgument(
          totalRecords > 0,
          "KeyBasedOnCountFn cannot be invoked on an element if there are no elements");
      if (totalRecords < MIN_SHARDS_FOR_LOG) {
        return Math.max(1L, totalRecords);
      }
      // 100mil records before >7 output files
      long floorLogRecs = Double.valueOf(Math.log10(totalRecords)).longValue();
      long shards = Math.max(floorLogRecs, MIN_SHARDS_FOR_LOG);
      return shards;
    }
  }
}
