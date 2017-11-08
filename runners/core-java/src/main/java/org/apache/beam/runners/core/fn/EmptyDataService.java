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

package org.apache.beam.runners.core.fn;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

/**
 */
class EmptyDataService implements FnDataService {
  @Override
  public <T> ListenableFuture<Void> listen(
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> listener)
      throws Exception {
    throw new UnsupportedOperationException(
        String.format("%s should not be used", getClass().getSimpleName()));
  }

  @Override
  public <T> FnDataReceiver<WindowedValue<T>> send(
      LogicalEndpoint outputLocation, Coder<WindowedValue<T>> coder) throws Exception {
    throw new UnsupportedOperationException(
        String.format("%s should not be used", getClass().getSimpleName()));
  }
}
