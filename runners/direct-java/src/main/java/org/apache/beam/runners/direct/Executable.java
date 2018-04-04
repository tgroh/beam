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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * An executable portion of the pipeline. This can represent either a single {@link PTransform}, an
 * {@link ExecutableStage}, or any other 'processing'
 */
abstract class Executable<T> {
  abstract String getId();
  abstract String getUrn();

  abstract T getUnderlying();

  public static Executable<RunnerApi.PTransform> fromProto(RunnerApi.PTransform pTransform) {
    throw new UnsupportedOperationException();
  }

  public static Executable<PTransform<?, ?>> fromJavaTransform(PTransform<?, ?> pTransform) {
    throw new UnsupportedOperationException();
  }

  public boolean equals(Object other) {
    return other instanceof Executable && ((Executable<?>) other).getId().equals(this.getId());
  }
}
