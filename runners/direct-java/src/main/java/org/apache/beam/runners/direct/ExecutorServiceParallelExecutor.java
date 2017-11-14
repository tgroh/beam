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

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link PipelineExecutor} that uses an underlying {@link ExecutorService} and {@link
 * EvaluationContext} to execute a {@link Pipeline}.
 */
final class ExecutorServiceParallelExecutor
    implements PipelineExecutor, BundleExecutor<CommittedBundle<?>, AppliedPTransform<?, ?, ?>> {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final int targetParallelism;
  private final ExecutorService executorService;

  private final DirectGraph graph;
  private final RootProviderRegistry rootProviderRegistry;
  private final TransformEvaluatorRegistry registry;

  private final EvaluationContext evaluationContext;

  private final TransformExecutorFactory<
          CommittedBundle<?>, AppliedPTransform<?, ?, ?>, TransformResult<?>>
      executorFactory;
  private final TransformExecutorService parallelExecutorService;
  private final LoadingCache<StepAndKey, TransformExecutorService> serialExecutorServices;

  private final BlockingQueue<VisibleExecutorUpdate> visibleUpdates;

  private final ConcurrentMap<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
      pendingRootBundles;

  private AtomicReference<State> pipelineState = new AtomicReference<>(State.RUNNING);

  public static ExecutorServiceParallelExecutor create(
      int targetParallelism,
      DirectGraph graph,
      RootProviderRegistry rootProviderRegistry,
      TransformEvaluatorRegistry registry,
      Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context) {
    return new ExecutorServiceParallelExecutor(
        targetParallelism,
        graph,
        rootProviderRegistry,
        registry,
        transformEnforcements,
        context);
  }

  private ExecutorServiceParallelExecutor(
      int targetParallelism,
      DirectGraph graph,
      RootProviderRegistry rootProviderRegistry,
      TransformEvaluatorRegistry registry,
      Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context) {
    this.targetParallelism = targetParallelism;
    // Don't use Daemon threads for workers. The Pipeline should continue to execute even if there
    // are no other active threads (for example, because waitUntilFinish was not called)
    this.executorService =
        Executors.newFixedThreadPool(
            targetParallelism,
            new ThreadFactoryBuilder()
                .setThreadFactory(MoreExecutors.platformThreadFactory())
                .setNameFormat("direct-runner-worker")
                .build());
    this.graph = graph;
    this.rootProviderRegistry = rootProviderRegistry;
    this.registry = registry;
    this.evaluationContext = context;

    // Weak Values allows TransformExecutorServices that are no longer in use to be reclaimed.
    // Executing TransformExecutorServices have a strong reference to their TransformExecutorService
    // which stops the TransformExecutorServices from being prematurely garbage collected
    serialExecutorServices =
        CacheBuilder.newBuilder()
            .weakValues()
            .removalListener(shutdownExecutorServiceListener())
            .build(serialTransformExecutorServiceCacheLoader());

    this.visibleUpdates = new LinkedBlockingQueue<>();

    parallelExecutorService = TransformExecutorServices.parallel(executorService);
    this.pendingRootBundles = new ConcurrentHashMap<>();
    executorFactory = new DirectTransformExecutor.Factory(context, registry, transformEnforcements);
  }

  private CacheLoader<StepAndKey, TransformExecutorService>
      serialTransformExecutorServiceCacheLoader() {
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
  public void start(Collection<AppliedPTransform<?, ?, ?>> roots) {
    int numTargetSplits = Math.max(3, targetParallelism);
    for (AppliedPTransform<?, ?, ?> root : roots) {
      ConcurrentLinkedQueue<CommittedBundle<?>> pending = new ConcurrentLinkedQueue<>();
      try {
        Collection<CommittedBundle<?>> initialInputs =
            rootProviderRegistry.getInitialInputs(root, numTargetSplits);
        pending.addAll(initialInputs);
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      }
      pendingRootBundles.put(root, pending);
    }
    evaluationContext.initialize(pendingRootBundles);
    Runnable monitorRunnable = new MonitorRunnable();
    executorService.submit(monitorRunnable);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void scheduleConsumption(
      AppliedPTransform<?, ?, ?> consumer,
      CommittedBundle<?> bundle,
      CompletionCallback<CommittedBundle<?>, AppliedPTransform<?, ?, ?>, ?> onComplete) {
    evaluateBundle(consumer, bundle, onComplete);
  }

  private <T> void evaluateBundle(
      AppliedPTransform<?, ?, ?> transform,
      CommittedBundle<T> bundle,
      CompletionCallback onComplete) {
    TransformExecutorService transformExecutor;

    if (isKeyed(bundle.getPCollection())) {
      final StepAndKey stepAndKey = StepAndKey.of(transform, bundle.getKey());
      // This executor will remain reachable until it has executed all scheduled transforms.
      // The TransformExecutors keep a strong reference to the Executor, the ExecutorService keeps
      // a reference to the scheduled DirectTransformExecutor callable. Follow-up TransformExecutors
      // (scheduled due to the completion of another DirectTransformExecutor) are provided to the
      // ExecutorService before the Earlier DirectTransformExecutor callable completes.
      transformExecutor = serialExecutorServices.getUnchecked(stepAndKey);
    } else {
      transformExecutor = parallelExecutorService;
    }

    TransformExecutor callable =
        executorFactory.create(bundle, transform, onComplete, transformExecutor);
    outstandingWork.incrementAndGet();
    if (!pipelineState.get().isTerminal()) {
      transformExecutor.schedule(callable);
    }
  }

  private boolean isKeyed(PValue pvalue) {
    return evaluationContext.isKeyed(pvalue);
  }

  @Override
  public State waitUntilFinish(Duration duration) throws Exception {
    Instant completionTime;
    if (duration.equals(Duration.ZERO)) {
      completionTime = new Instant(Long.MAX_VALUE);
    } else {
      completionTime = Instant.now().plus(duration);
    }

    VisibleExecutorUpdate update = null;
    while (Instant.now().isBefore(completionTime)
        && (update == null || isTerminalStateUpdate(update))) {
      // Get an update; don't block forever if another thread has handled it. The call to poll will
      // wait the entire timeout; this call primarily exists to relinquish any core.
      update = visibleUpdates.poll(25L, TimeUnit.MILLISECONDS);
      if (update == null && pipelineState.get().isTerminal()) {
        // there are no updates to process and no updates will ever be published because the
        // executor is shutdown
        return pipelineState.get();
      } else if (update != null && update.thrown.isPresent()) {
        Throwable thrown = update.thrown.get();
        if (thrown instanceof Exception) {
          throw (Exception) thrown;
        } else if (thrown instanceof Error) {
          throw (Error) thrown;
        } else {
          throw new Exception("Unknown Type of Throwable", thrown);
        }
      }
    }
    return pipelineState.get();
  }

  @Override
  public State getPipelineState() {
    return pipelineState.get();
  }

  private boolean isTerminalStateUpdate(VisibleExecutorUpdate update) {
    return !(update.getNewState() == null && update.getNewState().isTerminal());
  }

  @Override
  public void stop() {
    shutdownIfNecessary(State.CANCELLED);
    while (!visibleUpdates.offer(VisibleExecutorUpdate.cancelled())) {
      // Make sure "This Pipeline was Cancelled" notification arrives.
      visibleUpdates.poll();
    }
  }

  private void shutdownIfNecessary(State newState) {
    if (!newState.isTerminal()) {
      return;
    }
    LOG.debug("Pipeline has terminated. Shutting down.");
    pipelineState.compareAndSet(State.RUNNING, newState);
    // Stop accepting new work before shutting down the executor. This ensures that thread don't try
    // to add work to the shutdown executor.
    serialExecutorServices.invalidateAll();
    serialExecutorServices.cleanUp();
    parallelExecutorService.shutdown();
    executorService.shutdown();
    try {
      registry.cleanup();
    } catch (Exception e) {
      visibleUpdates.add(VisibleExecutorUpdate.fromException(e));
    }
  }

  /**
   * An update of interest to the user. Used in {@link #waitUntilFinish} to decide whether to
   * return normally or throw an exception.
   */
  private static class VisibleExecutorUpdate {
    private final Optional<? extends Throwable> thrown;
    @Nullable
    private final State newState;

    public static VisibleExecutorUpdate fromException(Exception e) {
      return new VisibleExecutorUpdate(null, e);
    }

    public static VisibleExecutorUpdate fromError(Error err) {
      return new VisibleExecutorUpdate(State.FAILED, err);
    }

    public static VisibleExecutorUpdate finished() {
      return new VisibleExecutorUpdate(State.DONE, null);
    }

    public static VisibleExecutorUpdate cancelled() {
      return new VisibleExecutorUpdate(State.CANCELLED, null);
    }

    private VisibleExecutorUpdate(State newState, @Nullable Throwable exception) {
      this.thrown = Optional.fromNullable(exception);
      this.newState = newState;
    }

    public State getNewState() {
      return newState;
    }
  }
}
