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

import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

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
        PTransform<InputT, OutputT> shardControlledWrite =
            (PTransform<InputT, OutputT>)
                that.withNumShards(3 + ThreadLocalRandom.current().nextInt(5));
        return shardControlledWrite;
      }
    }
    return transform;
  }
}
