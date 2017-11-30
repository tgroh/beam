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

package org.apache.beam.fn.harness.data;

import java.util.Collection;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A {@link FnDataReceiver} which forwards all received inputs to a collection of {@link
 * ThrowingConsumer consumers}.
 *
 * <p>Calls to {@link #close()} have no effect.
 */
public class MultiplexingFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {
  public static <T> MultiplexingFnDataReceiver<T> forConsumers(
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers) {
    return new MultiplexingFnDataReceiver<>(consumers);
  }

  private final Collection<ThrowingConsumer<WindowedValue<T>>> consumers;

  private MultiplexingFnDataReceiver(Collection<ThrowingConsumer<WindowedValue<T>>> consumers) {
    this.consumers = consumers;
  }

  @Override
  public void accept(WindowedValue<T> input) throws Exception {
    for (ThrowingConsumer<WindowedValue<T>> consumer : consumers) {
      consumer.accept(input);
    }
  }

  @Override
  public void close() throws Exception {
    // The consumers aren't closeable, so there is no work to be done here.
  }
}
