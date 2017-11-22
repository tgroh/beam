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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.runners.local.ExecutionDriver;
import org.apache.beam.runners.local.ExecutionDriver.DriverState;
import org.apache.beam.runners.local.PipelineMessageReceiver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link PipelineExecutor} that uses an underlying {@link ExecutorService} and {@link
 * EvaluationContext} to execute a {@link Pipeline}.
 */
final class ExecutorServiceParallelExecutor<BundleT extends Bundle<?>, ExecutableT, CallbackT>
    implements PipelineExecutor, BundleExecutor<BundleT, ExecutableT, CallbackT> {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final int targetParallelism;
  private final ExecutorService executorService;

  private final TransformEvaluatorRegistry registry;
  private final EvaluationContext evaluationContext;

  private final TransformExecutorFactory<BundleT, ExecutableT, CallbackT> executorFactory;
  private final TransformExecutorServiceProvider<BundleT, ExecutableT> executorServiceProvider;

  private final QueueMessageReceiver visibleUpdates;

  private AtomicReference<State> pipelineState = new AtomicReference<>(State.RUNNING);

  public static <BundleT extends Bundle<?>, ExecutableT, CallbackT>
      ExecutorServiceParallelExecutor<BundleT, ExecutableT, CallbackT> create(
          int targetParallelism,
          TransformEvaluatorRegistry registry,
          TransformExecutorServiceProvider<BundleT, ExecutableT> executorServiceProvider,
          TransformExecutorFactory<BundleT, ExecutableT, CallbackT> executorFactory,
          Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
          EvaluationContext context) {
    return new ExecutorServiceParallelExecutor<>(
        targetParallelism,
        registry,
        executorServiceProvider,
        executorFactory,
        transformEnforcements,
        context);
  }

  private ExecutorServiceParallelExecutor(
      int targetParallelism,
      TransformEvaluatorRegistry registry,
      TransformExecutorServiceProvider<BundleT, ExecutableT> executorServiceProvider,
      TransformExecutorFactory executorFactory,
      Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context) {
    this.evaluationContext = context;
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
    this.executorServiceProvider = executorServiceProvider;
    this.registry = registry;

    this.visibleUpdates = new QueueMessageReceiver();

    this.executorFactory = executorFactory;
  }

  @Override
  public void start(DirectGraph graph, RootProviderRegistry rootProviderRegistry) {
    int numTargetSplits = Math.max(3, targetParallelism);
    ImmutableMap.Builder<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
        pendingRootBundles = ImmutableMap.builder();
    for (AppliedPTransform<?, ?, ?> root : graph.getRootTransforms()) {
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
    evaluationContext.initialize(pendingRootBundles.build());
    final ExecutionDriver executionDriver =
        QuiescenceDriver.create(
            evaluationContext, graph, this, visibleUpdates, pendingRootBundles.build());
    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            DriverState drive = executionDriver.drive();
            if (drive.isTermainal()) {
              State newPipelineState = State.UNKNOWN;
              switch (drive) {
                case FAILED:
                  newPipelineState = State.FAILED;
                  break;
                case SHUTDOWN:
                  newPipelineState = State.DONE;
                  break;
                case CONTINUE:
                  throw new IllegalStateException(
                      String.format("%s should not be a terminal state", DriverState.CONTINUE));
                default:
                  throw new IllegalArgumentException(
                      String.format("Unknown %s %s", DriverState.class.getSimpleName(), drive));
              }
              shutdownIfNecessary(newPipelineState);
            } else {
              executorService.submit(this);
            }
          }
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(
      BundleT bundle,
      ExecutableT consumer,
      CallbackT onComplete) {
    TransformExecutorService transformExecutor =
        executorServiceProvider.getExecutor(consumer, bundle);

    TransformExecutor callable =
        executorFactory.create(bundle, consumer, onComplete, transformExecutor);
    if (!pipelineState.get().isTerminal()) {
      transformExecutor.schedule(callable);
    }
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
      update = visibleUpdates.tryNext(Duration.millis(25L));
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
    visibleUpdates.cancelled();
  }

  private void shutdownIfNecessary(State newState) {
    if (!newState.isTerminal()) {
      return;
    }
    LOG.debug("Pipeline has terminated. Shutting down.");
    pipelineState.compareAndSet(State.RUNNING, newState);
    // Stop accepting new work before shutting down the executor. This ensures that thread don't try
    // to add work to the shutdown executor.
    try {
      executorServiceProvider.close();
    } catch (Exception e) {
      visibleUpdates.failed(e);
    }
    executorService.shutdown();
    try {
      registry.cleanup();
    } catch (Exception e) {
      visibleUpdates.failed(e);
    }
  }

  /**
   * An update of interest to the user. Used in {@link #waitUntilFinish} to decide whether to return
   * normally or throw an exception.
   */
  private static class VisibleExecutorUpdate {
    private final Optional<? extends Throwable> thrown;
    @Nullable private final State newState;

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

    State getNewState() {
      return newState;
    }
  }

  private static class QueueMessageReceiver implements PipelineMessageReceiver {
    private final BlockingQueue<VisibleExecutorUpdate> updates = new LinkedBlockingQueue<>();

    @Override
    public void failed(Exception e) {
      updates.offer(VisibleExecutorUpdate.fromException(e));
    }

    @Override
    public void failed(Error e) {
      updates.offer(VisibleExecutorUpdate.fromError(e));
    }

    @Override
    public void cancelled() {
      updates.offer(VisibleExecutorUpdate.cancelled());
    }

    @Override
    public void completed() {
      updates.offer(VisibleExecutorUpdate.finished());
    }

    /**
     * Try to get the next unconsumed message in this {@link QueueMessageReceiver}.
     */
    @Nullable
    private VisibleExecutorUpdate tryNext(Duration timeout) throws InterruptedException {
      return updates.poll(timeout.getMillis(), TimeUnit.MILLISECONDS);
    }
  }
}
