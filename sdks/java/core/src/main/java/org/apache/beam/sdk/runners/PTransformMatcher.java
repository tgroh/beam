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

package org.apache.beam.sdk.runners;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A Function that takes {@link PTransform PTransforms} and determines if they match some condition.
 */
@Experimental(Kind.RUNNER_API)
public interface PTransformMatcher {
  /** Determines how the provided {@link PTransform} should be treated. */
  Match match(PTransform<?, ?> transform);

  enum Match {
    /**
     * A composite {@link PTransform} should be entered, and a primitive {@link PTransform} should
     * be passed over. This should be returned when the {@link PipelineRunner} has no replacement
     * node to apply to the {@link PTransform}.
     */
    CONTINUE,
    /** The {@link PTransform} should be replaced by a runner-specific representation. */
    REPLACE,
    /**
     * The {@link PTransform} should be passed over. If the {@link PTransform} is a composite, it
     * should not be entered.
     */
    SKIP
  }
}
