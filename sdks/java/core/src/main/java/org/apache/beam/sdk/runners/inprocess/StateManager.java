/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
 *
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A class that manages {@link CopyOnAccessInMemoryStateInternals} for the
 * {@link InProcessPipelineRunner}.
 *
 * <p>Execution of a StepAndKey that interacts with state may not occur concurrently. This
 * implementation assumes that this restriction always holds.
 */
class StateManager {
  public static StateManager create() {
    return new StateManager();
  }

  /** The stateInternals of the world, by applied PTransform and key. */
  private final LoadingCache<StepAndKey, CopyOnAccessInMemoryStateInternals<?>> stateCache;

  private StateManager() {
    stateCache = CacheBuilder.newBuilder().build(new NewStateInternalsLoader());
  }

  public CopyOnAccessInMemoryStateInternals<?> get(StepAndKey stepAndKey) {
    return stateCache.getUnchecked(stepAndKey);
  }

  public void commit(StepAndKey stepAndKey, CopyOnAccessInMemoryStateInternals<?> state) {
    CopyOnAccessInMemoryStateInternals<?> committedState = state.commit();
    if (committedState.isEmpty()) {
      // Remove the old value, to allow it to be GCed if there are not more states for this step and
      // key
      stateCache.invalidate(stepAndKey);
    } else {
      // overwrites any existing value
      stateCache.put(stepAndKey, committedState);
    }
  }

  private static class NewStateInternalsLoader
      extends CacheLoader<StepAndKey, CopyOnAccessInMemoryStateInternals<?>> {
    @Override
    public CopyOnAccessInMemoryStateInternals<?> load(StepAndKey key) throws Exception {
      return CopyOnAccessInMemoryStateInternals.withUnderlying(key.getKey(), null);
    }
  }
}
