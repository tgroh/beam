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

<<<<<<< HEAD
import java.util.concurrent.ExecutorService;

/**
 * A factory that creates {@link ExecutorService ExecutorServices}.
 * {@link ExecutorService ExecutorServices} created by this factory should be independent of one
 * another (e.g., if any executor is shut down the remaining executors should continue to process
 * work).
 */
public interface ExecutorServiceFactory {
  /**
   * Create a new {@link ExecutorService}.
   */
  ExecutorService create();
}

=======
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A factory for creating ExecutorServices.
 */
public interface ExecutorServiceFactory {

  /** Create a new ExecutorService. */
  ExecutorService create();
  /**
   * A {@link DefaultValueFactory} that produces cached thread pools via
   * {@link Executors#newCachedThreadPool()}.
   */
  class WorkStealingPoolExecutorServiceFactoryFactory
      implements DefaultValueFactory<ExecutorServiceFactory> {
    @Override
    public ExecutorServiceFactory create(PipelineOptions options) {
      return new WorkStealingPoolExecutorServiceFactory();
    }

    class WorkStealingPoolExecutorServiceFactory implements ExecutorServiceFactory {
      @Override
      public ExecutorService create() {
        ExecutorService service = Executors.newCachedThreadPool();
        return service;
      }
    }
  }
}
>>>>>>> f94a1b9... WIP:
