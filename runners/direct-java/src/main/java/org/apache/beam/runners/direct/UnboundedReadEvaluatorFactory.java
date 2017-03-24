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

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
class UnboundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadEvaluatorFactory.class);
  // Occasionally close an existing reader and resume from checkpoint, to exercise close-and-resume
  private static final double DEFAULT_READER_REUSE_CHANCE = 0.95;

  private final EvaluationContext evaluationContext;
  private final double readerReuseChance;

  UnboundedReadEvaluatorFactory(EvaluationContext evaluationContext) {
    this(evaluationContext, DEFAULT_READER_REUSE_CHANCE);
  }

  @VisibleForTesting
  UnboundedReadEvaluatorFactory(EvaluationContext evaluationContext, double readerReuseChance) {
    this.evaluationContext = evaluationContext;
    this.readerReuseChance = readerReuseChance;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  @Nullable
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    return createEvaluator((AppliedPTransform) application);
  }

  private <OutputT> TransformEvaluator<?> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, Read.Unbounded<OutputT>> application) {
    return new UnboundedReadEvaluator<>(application, evaluationContext, readerReuseChance);
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
  private static class UnboundedReadEvaluator<OutputT, CheckpointMarkT extends CheckpointMark>
      implements TransformEvaluator<UnboundedSourceShard<OutputT, CheckpointMarkT>> {
    private static final int ARBITRARY_MAX_ELEMENTS = 10;

    private final AppliedPTransform<?, PCollection<OutputT>, ?> transform;
    private final EvaluationContext evaluationContext;
    private final double readerReuseChance;
    private final StepTransformResult.Builder resultBuilder;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, ?> transform,
        EvaluationContext evaluationContext,
        double readerReuseChance) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.readerReuseChance = readerReuseChance;
      resultBuilder = StepTransformResult.withoutHold(transform);
    }

    @Override
    public void processElement(
        WindowedValue<UnboundedSourceShard<OutputT, CheckpointMarkT>> element) throws IOException {
      UncommittedBundle<OutputT> output =
          evaluationContext.createBundle(
              (PCollection<OutputT>) getOnlyElement(transform.getOutputs()).getValue());
      UnboundedSourceShard<OutputT, CheckpointMarkT> shard = element.getValue();
      UnboundedReader<OutputT> reader = null;
      try {
        reader = getReader(shard);
        boolean elementAvailable = startReader(reader, shard);

        if (elementAvailable) {
          UnboundedReadDeduplicator deduplicator = shard.getDeduplicator();
          int numElements = 0;
          do {
            if (deduplicator.shouldOutput(reader.getCurrentRecordId())) {
              output.add(
                  WindowedValue.timestampedValueInGlobalWindow(
                      reader.getCurrent(), reader.getCurrentTimestamp()));
            }
            numElements++;
          } while (numElements < ARBITRARY_MAX_ELEMENTS && reader.advance());
          Instant watermark = reader.getWatermark();

          CheckpointMarkT finishedCheckpoint = finishRead(reader, shard);
          UnboundedSourceShard<OutputT, CheckpointMarkT> residual;
          // Sometimes resume from a checkpoint even if it's not required
          if (ThreadLocalRandom.current().nextDouble(1.0) >= readerReuseChance) {
            UnboundedReader<OutputT> toClose = reader;
            // Prevent double-close. UnboundedReader is AutoCloseable, which does not require
            // idempotency of close. Nulling out the reader here prevents trying to re-close it
            // if the call to close throws an IOException.
            reader = null;
            toClose.close();
            residual =
                UnboundedSourceShard.of(
                    shard.getSource(),
                    shard.getDeduplicator(),
                    Resumption.<OutputT>forReader(
                        CoderUtils.encodeToByteArray(
                            shard.getSource().getCheckpointMarkCoder(), finishedCheckpoint)),
                    finishedCheckpoint);
          } else {
            residual = shard.withCheckpoint(finishedCheckpoint);
          }

          resultBuilder
              .addOutput(output)
              .addUnprocessedElements(
                  Collections.singleton(
                      WindowedValue.timestampedValueInGlobalWindow(residual, watermark)));
        } else if (reader.getWatermark().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
          // If the reader had no elements available, but the shard is not done, reuse it later
          resultBuilder.addUnprocessedElements(
              Collections.<WindowedValue<?>>singleton(
                  WindowedValue.timestampedValueInGlobalWindow(
                      UnboundedSourceShard.of(
                          shard.getSource(),
                          shard.getDeduplicator(),
                          Resumption.forReader(reader),
                          shard.getCheckpoint()),
                      reader.getWatermark())));
        }
      } catch (IOException e) {
        if (reader != null) {
          reader.close();
        }
        throw e;
      }
    }

    private UnboundedReader<OutputT> getReader(UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      if (shard.getResumption() == null || shard.getResumption().getReader() == null) {
        CheckpointMarkT checkpoint = null;
        if (shard.getResumption() != null && shard.getResumption().getEncodedCheckpoint() != null) {
          checkpoint =
              CoderUtils.decodeFromByteArray(
                  shard.getSource().getCheckpointMarkCoder(),
                  shard.getResumption().getEncodedCheckpoint());
        }
        return shard
            .getSource()
            .createReader(evaluationContext.getPipelineOptions(), checkpoint);
      } else {
        return shard.getResumption().getReader();
      }
    }

    private boolean startReader(
        UnboundedReader<OutputT> reader, UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      if (shard.getResumption() == null || shard.getResumption().getReader() == null) {
        return reader.start();
      } else {
        return shard.getResumption().getReader().advance();
      }
    }

    /**
     * Checkpoint the current reader, finalize the previous checkpoint, and return the residual
     * {@link UnboundedSourceShard}.
     */
    private CheckpointMarkT finishRead(
        UnboundedReader<OutputT> reader, UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      final CheckpointMark oldMark = shard.getCheckpoint();
      @SuppressWarnings("unchecked")
      final CheckpointMarkT mark = (CheckpointMarkT) reader.getCheckpointMark();
      if (oldMark != null) {
        oldMark.finalizeCheckpoint();
      }

      // If the watermark is the max value, this source may not be invoked again. Finalize after
      // committing the output.
      if (!reader.getWatermark().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        PCollection<OutputT> outputPc =
            (PCollection<OutputT>) Iterables.getOnlyElement(transform.getOutputs()).getValue();
        evaluationContext.scheduleAfterOutputWouldBeProduced(
            outputPc,
            GlobalWindow.INSTANCE,
            outputPc.getWindowingStrategy(),
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
      return mark;
    }

    @Override
    public TransformResult<UnboundedSourceShard<OutputT, CheckpointMarkT>> finishBundle()
        throws IOException {
      return resultBuilder.build();
    }
  }

  @AutoValue
  abstract static class UnboundedSourceShard<T, CheckpointT extends CheckpointMark> {
    static <T, CheckpointT extends CheckpointMark> UnboundedSourceShard<T, CheckpointT> unstarted(
        UnboundedSource<T, CheckpointT> source, UnboundedReadDeduplicator deduplicator) {
      return of(source, deduplicator, null, null);
    }

    static <T, CheckpointT extends CheckpointMark> UnboundedSourceShard<T, CheckpointT> of(
        UnboundedSource<T, CheckpointT> source,
        UnboundedReadDeduplicator deduplicator,
        @Nullable Resumption<T> resumption,
        @Nullable CheckpointT checkpoint) {
      return new AutoValue_UnboundedReadEvaluatorFactory_UnboundedSourceShard<>(
          source, deduplicator, resumption, checkpoint);
    }

    abstract UnboundedSource<T, CheckpointT> getSource();

    abstract UnboundedReadDeduplicator getDeduplicator();

    @Nullable
    abstract Resumption<T> getResumption();
    @Nullable
    abstract CheckpointT getCheckpoint();

    UnboundedSourceShard<T, CheckpointT> withCheckpoint(CheckpointT newCheckpoint) {
      return of(getSource(), getDeduplicator(), getResumption(), newCheckpoint);
    }
  }

  @AutoValue
  abstract static class Resumption<T> {
    public static <T> Resumption<T> forReader(UnboundedReader<T> reader) {
      return new AutoValue_UnboundedReadEvaluatorFactory_Resumption<T>(reader, null);
    }

    public static <T> Resumption<T> forReader(byte[] checkpoint) {
      return new AutoValue_UnboundedReadEvaluatorFactory_Resumption<T>(null, checkpoint);
    }

    @Nullable
    abstract UnboundedReader<T> getReader();
    @Nullable
    abstract byte[] getEncodedCheckpoint();
  }

  static class InputProvider<OutputT>
      implements RootInputProvider<
          OutputT, UnboundedSourceShard<OutputT, ?>, PBegin, Unbounded<OutputT>> {
    private final EvaluationContext evaluationContext;

    InputProvider(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Collection<CommittedBundle<UnboundedSourceShard<OutputT, ?>>> getInitialInputs(
        AppliedPTransform<PBegin, PCollection<OutputT>, Unbounded<OutputT>> transform,
        int targetParallelism)
        throws Exception {
      UnboundedSource<OutputT, ?> source = transform.getTransform().getSource();
      List<? extends UnboundedSource<OutputT, ?>> splits =
          source.generateInitialSplits(targetParallelism, evaluationContext.getPipelineOptions());
      UnboundedReadDeduplicator deduplicator =
          source.requiresDeduping()
              ? UnboundedReadDeduplicator.CachedIdDeduplicator.create()
              : NeverDeduplicator.create();

      ImmutableList.Builder<CommittedBundle<UnboundedSourceShard<OutputT, ?>>> initialShards =
          ImmutableList.builder();
      for (UnboundedSource<OutputT, ?> split : splits) {
        UnboundedSourceShard<OutputT, ?> shard =
            UnboundedSourceShard.unstarted(split, deduplicator);
        initialShards.add(
            evaluationContext
                .<UnboundedSourceShard<OutputT, ?>>createRootBundle()
                .add(WindowedValue.<UnboundedSourceShard<OutputT, ?>>valueInGlobalWindow(shard))
                .commit(BoundedWindow.TIMESTAMP_MAX_VALUE));
      }
      return initialShards.build();
    }
  }
}
