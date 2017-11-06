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

package org.apache.beam.runners.fnexecution.control;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;

/**
 * Created by tgroh on 11/3/17.
 */
public class ActiveBundle {
  private final String bundleId;
  private final FnControlClientHandler controlClientHandler;

  public ActiveBundle(
      String bundleId, FnControlClientHandler controlClientHandler) {
    this.bundleId = bundleId;
    this.controlClientHandler = controlClientHandler;
  }

  public boolean isComplete() {
    return false;
  }

  void completeSuccessfully() {}
  void completeExceptionally(Exception e) {}
}
