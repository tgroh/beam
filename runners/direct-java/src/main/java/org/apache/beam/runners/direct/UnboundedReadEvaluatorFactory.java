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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.CachedIdDeduplicator;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.NeverDeduplicator;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
class UnboundedReadEvaluatorFactory implements RootTransformEvaluatorFactory {
  private final ConcurrentMap<AppliedPTransform<?, ?, ?>, UnboundedReadDeduplicator> deduplicators;
  private final EvaluationContext evaluationContext;

  UnboundedReadEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    deduplicators = new ConcurrentHashMap<>();
  }

  @Override
  public List<CommittedBundle<?>> getInitialInputs(AppliedPTransform<?, ?, ?> transform) {
    return getSplits((AppliedPTransform) transform);
  }

  public <T, CheckpointMarkT extends CheckpointMark>
      List<CommittedBundle<UnboundedSourceShard<T, CheckpointMarkT>>> getSplits(
          AppliedPTransform<?, ?, Read.Unbounded<T>> transform) {
    if (transform.getTransform().getSource().requiresDeduping()) {
      deduplicators.put(transform, CachedIdDeduplicator.create());
    } else {
      deduplicators.put(transform, NeverDeduplicator.create());
    }
    ImmutableList.Builder<CommittedBundle<UnboundedSourceShard<T, CheckpointMarkT>>> shards =
        ImmutableList.builder();
    // TODO: Perform initial splitting
    @SuppressWarnings("unchecked") // The actual type of checkpoint marks doesn't matter
    UnboundedSource<T, CheckpointMarkT> source =
        (UnboundedSource<T, CheckpointMarkT>) transform.getTransform().getSource();

    UncommittedBundle<UnboundedSourceShard<T, CheckpointMarkT>> bundle =
        evaluationContext.createRootBundle();
    bundle.add(WindowedValue.valueInGlobalWindow(UnboundedSourceShard.unstarted(source)));
    shards.add(bundle.commit(BoundedWindow.TIMESTAMP_MIN_VALUE));

    return shards.build();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  @Nullable
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, @Nullable CommittedBundle<?> inputBundle) {
    return (TransformEvaluator<InputT>)
        createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
  }

  private <OutputT, CheckpointT extends CheckpointMark>
      UnboundedReadEvaluator<OutputT, CheckpointT> createEvaluator(
          AppliedPTransform<?, PCollection<OutputT>, Read.Unbounded<OutputT>> application,
          CommittedBundle<UnboundedSourceShard<OutputT, CheckpointT>> inputBundle) {
    throw new IllegalArgumentException();
  }

  @Override
  public void cleanup() {}

  /**
   * A {@link UnboundedReadEvaluator} produces elements from an underlying {@link UnboundedSource},
   * discarding all input elements. Within the call to {@link #finishBundle()}, the evaluator
   * creates the {@link UnboundedReader} and consumes some currently available input.
   *
   * <p>Calls to {@link UnboundedReadEvaluator} are not internally thread-safe, and should only be
   * used by a single thread at a time. Each {@link UnboundedReadEvaluator} maintains its own
   * checkpoint, and constructs its reader from the current checkpoint in each call to {@link
   * #finishBundle()}.
   */
  @VisibleForTesting
  static class UnboundedReadEvaluator<OutputT, CheckpointMarkT extends CheckpointMark>
      implements TransformEvaluator<UnboundedSourceShard<OutputT, CheckpointMarkT>> {
    @VisibleForTesting
    static final long ARBITRARY_MAX_ELEMENTS = 250;
    // == 5% chance to throw away a functional reader and resume from a cloned checkpoint.
    private static final double REUSE_CHANCE = 0.95;

    private final AppliedPTransform<?, PCollection<OutputT>, Read.Unbounded<OutputT>> transform;
    private final EvaluationContext evaluationContext;
    private final UnboundedReadDeduplicator deduplicator;

    private final StepTransformResult.Builder resultBuilder;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, Read.Unbounded<OutputT>> transform,
        EvaluationContext evaluationContext,
        UnboundedReadDeduplicator deduplicator) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.deduplicator = deduplicator;
      resultBuilder = StepTransformResult.withoutHold(transform);
    }

    @Override
    public void processElement(
        WindowedValue<UnboundedSourceShard<OutputT, CheckpointMarkT>> element) throws IOException {
      UncommittedBundle<OutputT> output = evaluationContext.createRootBundle();
      UnboundedSourceShard<OutputT, CheckpointMarkT> shard = element.getValue();
      try (UnboundedReader<OutputT> reader = getReader(shard)) {
        boolean elementAvailable = startReader(shard, reader);

        if (elementAvailable) {
          int numElements = 0;
          do {
            if (deduplicator.shouldOutput(reader.getCurrentRecordId())) {
              output.add(
                  WindowedValue.timestampedValueInGlobalWindow(
                      reader.getCurrent(), reader.getCurrentTimestamp()));
            }
            numElements++;
          } while (numElements < ARBITRARY_MAX_ELEMENTS && reader.advance());
          resultBuilder.addOutput(output);
        }
        WindowedValue<UnboundedSourceShard<OutputT, CheckpointMarkT>> remaining =
            finishRead(shard, reader);
        resultBuilder.addUnprocessedElements(Collections.singleton(remaining));
        // TODO: When retries exist, the checkpoint for the input element must not get lost, and the
        // reader cannot be reused.
      }
    }

    @Override
    public TransformResult finishBundle() throws IOException {
      return resultBuilder.build();
    }

    private UnboundedReader<OutputT> getReader(UnboundedSourceShard<OutputT, CheckpointMarkT> value)
        throws IOException {
      if (value.getReader() == null) {
        return value
            .getSource()
            .createReader(evaluationContext.getPipelineOptions(), value.getCheckpoint());
      } else {
        return value.getReader();
      }
    }

    private boolean startReader(
        UnboundedSourceShard<OutputT, CheckpointMarkT> shard,
        UnboundedReader<OutputT> reader)
        throws IOException {
      if (shard.getReader() == null) {
        if (shard.getCheckpoint() != null) {
          shard.getCheckpoint().finalizeCheckpoint();
        }
        return reader.start();
      } else {
        return reader.advance();
      }
    }

    /**
     * Checkpoint the current reader, finalize the previous checkpoint, and return a
     * {@link UnboundedSourceShard} that is suitable for resuming this read.
     */
    private WindowedValue<UnboundedSourceShard<OutputT, CheckpointMarkT>> finishRead(
        UnboundedSourceShard<OutputT, CheckpointMarkT> originalShard,
        UnboundedReader<OutputT> reader)
        throws IOException {
      final CheckpointMark oldMark = originalShard.getCheckpoint();
      @SuppressWarnings("unchecked")
      final CheckpointMarkT mark = (CheckpointMarkT) reader.getCheckpointMark();
      if (oldMark != null) {
        oldMark.finalizeCheckpoint();
      }

      // If the watermark is the max value, this source may not be invoked again. Finalize after
      // committing the output.
      if (!reader.getWatermark().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        evaluationContext.scheduleAfterOutputWouldBeProduced(
            transform.getOutput(),
            GlobalWindow.INSTANCE,
            transform.getOutput().getWindowingStrategy(),
            new Runnable() {
              @Override
              public void run() {
                try {
                  mark.finalizeCheckpoint();
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Couldn't finalize checkpoint after the end of the Global Window", e);
                }
              }
            });
      }
      // Sometimes resume from a checkpoint even if it's not required
      if (Math.random() < REUSE_CHANCE) {
        return WindowedValue.timestampedValueInGlobalWindow(
            UnboundedSourceShard.of(originalShard.getSource(), reader, mark),
            reader.getWatermark());
      } else {
        return WindowedValue.timestampedValueInGlobalWindow(
            UnboundedSourceShard.of(
                originalShard.getSource(),
                null,
                CoderUtils.clone(originalShard.getSource().getCheckpointMarkCoder(), mark)),
            reader.getWatermark());
      }
    }
  }

  @AutoValue
  abstract static class UnboundedSourceShard<T, CheckpointMarkT extends CheckpointMark> {
    public static <T, CheckpointMarkT extends CheckpointMark>
        UnboundedSourceShard<T, CheckpointMarkT> of(
            UnboundedSource<T, CheckpointMarkT> source,
            UnboundedReader<T> reader,
            CheckpointMarkT checkpoint) {
      return new AutoValue_UnboundedReadEvaluatorFactory_UnboundedSourceShard<>(
          source, reader, checkpoint);
    }

    public static <T, CheckpointMarkT extends CheckpointMark>
        UnboundedSourceShard<T, CheckpointMarkT> unstarted(
            UnboundedSource<T, CheckpointMarkT> source) {
      return of(source, null, null);
    }

    abstract UnboundedSource<T, CheckpointMarkT> getSource();

    @Nullable
    abstract UnboundedReader<T> getReader();

    @Nullable
    abstract CheckpointMarkT getCheckpoint();
  }
}
