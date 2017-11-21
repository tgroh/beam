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

package org.apache.beam.runners.reference;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.runners.local.TimerUpdate;
import org.apache.beam.runners.local.WatermarkHold;

/**
 * The result of executing a {@link PipelineStage}.
 */
public abstract class PipelineStageExecutionResult {
  abstract PipelineStage getStage();
  abstract Iterable<? extends Bundle<?>> getOutputBundles();
  // TODO: How are unprocessable elements handled.
//  private final ImmutableList.Builder<WindowedValue<InputT>> unprocessedElementsBuilder;
  // TODO: How are
//  private MetricUpdates metricUpdates;
//  abstract StateUpdates getStateUpdates();
  abstract Map<PTransform, TimerUpdate> getTimerUpdates();
  abstract Map<PTransform, WatermarkHold> getWatermarkHoldUpdates();
}
