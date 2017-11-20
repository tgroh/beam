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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.ExecutorService;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * An {@link TransformExecutorServiceProvider} which uses the key of an input bundle and if that
 * bundle's {@link PCollection} is keyed to provide executor services.
 */
public class KeyCheckingExecutorServiceProvider
    implements TransformExecutorServiceProvider<CommittedBundle<?>, AppliedPTransform<?, ?, ?>> {
  public static KeyCheckingExecutorServiceProvider create(
      EvaluationContext evaluationContext, ExecutorService executorService) {
    return new KeyCheckingExecutorServiceProvider(evaluationContext, executorService);
  }

  private final EvaluationContext evaluationContext;

  private final TransformExecutorService parallelExecutorService;
  private final LoadingCache<StepAndKey, TransformExecutorService> serialExecutorServices;

  private KeyCheckingExecutorServiceProvider(
      EvaluationContext evaluationContext, ExecutorService executorService) {
    this.evaluationContext = evaluationContext;

    // Weak Values allows TransformExecutorServices that are no longer in use to be reclaimed.
    // Executing TransformExecutorServices have a strong reference to their TransformExecutorService
    // which stops the TransformExecutorServices from being prematurely garbage collected
    serialExecutorServices =
        CacheBuilder.newBuilder()
            .weakValues()
            .removalListener(shutdownExecutorServiceListener())
            .build(serialTransformExecutorServiceCacheLoader(executorService));
    parallelExecutorService = TransformExecutorServices.parallel(executorService);
  }

  private CacheLoader<StepAndKey, TransformExecutorService>
      serialTransformExecutorServiceCacheLoader(final ExecutorService executorService) {
    return new CacheLoader<StepAndKey, TransformExecutorService>() {
      @Override
      public TransformExecutorService load(StepAndKey stepAndKey) throws Exception {
        return TransformExecutorServices.serial(executorService);
      }
    };
  }

  private RemovalListener<StepAndKey, TransformExecutorService> shutdownExecutorServiceListener() {
    return new RemovalListener<StepAndKey, TransformExecutorService>() {
      @Override
      public void onRemoval(
          RemovalNotification<StepAndKey, TransformExecutorService> notification) {
        TransformExecutorService service = notification.getValue();
        if (service != null) {
          service.shutdown();
        }
      }
    };
  }

  @Override
  public TransformExecutorService getExecutor(
      AppliedPTransform<?, ?, ?> transform, CommittedBundle<?> bundle, StructuralKey<?> key) {
    if (evaluationContext.isKeyed(bundle.getPCollection())) {
      final StepAndKey stepAndKey = StepAndKey.of(transform, bundle.getKey());
      // This executor will remain reachable until it has executed all scheduled transforms.
      // The TransformExecutors keep a strong reference to the Executor, the ExecutorService keeps
      // a reference to the scheduled DirectTransformExecutor callable. Follow-up TransformExecutors
      // (scheduled due to the completion of another DirectTransformExecutor) are provided to the
      // ExecutorService before the Earlier DirectTransformExecutor callable completes.
      return serialExecutorServices.getUnchecked(stepAndKey);
    } else {
      return parallelExecutorService;
    }
  }

  @Override
  public void close() throws Exception {
    serialExecutorServices.invalidateAll();
    serialExecutorServices.cleanUp();
    parallelExecutorService.shutdown();
  }
}
