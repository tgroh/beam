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

import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.SerializableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages {@link DoFn} setup, teardown, and serialization.
 *
 * <p>{@link DoFnLifecycleManager} is similar to a {@link ThreadLocal} storing a
 * {@link DoFn}, but calls {@link DoFn#setup()} the first time the {@link DoFn} is obtained,
 * {@link DoFn#teardown()} whenever the {@link DoFn} is removed, and provides a method for clearing
 * all cached {@link DoFn DoFns}.
 */
class DoFnLifecycleManager {
  public static DoFnLifecycleManager of(DoFn<?, ?> original) {
    return new DoFnLifecycleManager(original);
  }

  private final byte[] original;
  private final ConcurrentMap<Thread, DoFn<?, ?>> outstanding;

  private DoFnLifecycleManager(DoFn<?, ?> original) {
    this.original = SerializableUtils.serializeToByteArray(original);
    this.outstanding = new ConcurrentHashMap<>();
  }

  public DoFn<?, ?> get() throws Exception {
    Thread currentThread = Thread.currentThread();
    DoFn<?, ?> fn = outstanding.get(currentThread);
    if (fn == null) {
      // The key is the current thread, so this is safe when accessed in a multithreaded manner.
      fn =
          (DoFn<?, ?>)
              SerializableUtils.deserializeFromByteArray(
                  original, "DoFn Copy in thread " + currentThread.getName());
      outstanding.put(currentThread, fn);
      fn.setup();
    }
    return fn;
  }

  public void remove() throws Exception {
    Thread currentThread = Thread.currentThread();
    DoFn<?, ?> fn = outstanding.remove(currentThread);
    if (fn != null) {
      fn.teardown();
    }
  }

  /**
   * Remove all {@link DoFn DoFns} from this {@link DoFnLifecycleManager}. Returns all exceptions
   * that were thrown while calling the remove methods.
   *
   * <p>If the returned Collection is nonempty, an exception was thrown from at least one
   * {@link DoFn#teardown()} method, and the {@link PipelineRunner} should throw an exception.
   */
  public Collection<Exception> removeAll() {
    List<Exception> suppressed = new ArrayList<>();
    Iterator<Entry<Thread, DoFn<?, ?>>> entries = outstanding.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<Thread, DoFn<?, ?>> entry = entries.next();
      entries.remove();
      try {
        entry.getValue().teardown();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        suppressed.add(e);
      }
    }
    return suppressed;
  }
}
