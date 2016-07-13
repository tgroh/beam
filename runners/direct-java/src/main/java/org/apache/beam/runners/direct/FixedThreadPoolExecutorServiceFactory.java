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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Produces instances {@link ExecutorServiceFactory} that produces fixed thread pools via
 * {@link Executors#newFixedThreadPool(int)}, with the number of threads equal to the maximum
 * parallelism in {@link DirectOptions}.
 */
class FixedThreadPoolExecutorServiceFactory implements DefaultValueFactory<ExecutorServiceFactory> {

  @Override
  public ExecutorServiceFactory create(PipelineOptions options) {
    return new FixedParallelismThreadPoolFactory(
        options.as(DirectOptions.class).getMaxParallelism());
  }

  private static class FixedParallelismThreadPoolFactory implements ExecutorServiceFactory {
    private final int parallelism;

    private FixedParallelismThreadPoolFactory(int parallelism) {
      checkArgument(parallelism > 0, "Max Parallelism must be at least 1");
      this.parallelism = parallelism;
    }

    @Override
    public ExecutorService create() {
      return Executors.newFixedThreadPool(parallelism);
    }
  }
}
