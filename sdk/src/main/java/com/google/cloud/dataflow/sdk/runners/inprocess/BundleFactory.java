/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * A factory that creates {@link UncommittedBundle UncommittedBundles}.
 */
public interface BundleFactory {
  /**
   * Create a {@link UncommittedBundle} for use by a source.
   */
  public <T> UncommittedBundle<T> createRootBundle(PCollection<T> output);

  /**
   * Create a {@link UncommittedBundle} whose elements belong to the specified {@link
   * PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output);

  /**
   * Create a {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@link InProcessGroupByKeyOnly} {@link PTransform PTransforms}.
   */
  public <T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, Object key, PCollection<T> output);
}

