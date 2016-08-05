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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.auto.value.AutoValue;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * An Testing input that generates an unbounded {@link PCollection} of elements, advancing the
 * watermark and processing time as elements are emitted. After all of the specified elements are
 * emitted, ceases to produce output.
 *
 * <p>To use, TBD.
 */
public class CreateStream<T> extends PTransform<PBegin, PCollection<T>> {
  private final List<TestSourceEvent<T>> events;
  private final TypeDescriptor<T> typeDescriptor;
  private TestClock clock;

  public static <T> Builder<T> create(TypeDescriptor<T> td) {
    return new Builder<>(td, Optional.<PipelineOptions>absent());
  }

  private CreateStream(
      TypeDescriptor<T> typeDescriptor,
      List<TestSourceEvent<T>> events) {
    this.typeDescriptor = typeDescriptor;
    this.events = checkNotNull(events);
  }

  /**
   * An incomplete {@link CreateStream}. Elements added to this builder will be produced in
   * sequence when the pipeline created by the {@link CreateStream} is run.
   */
  public static class Builder<T> {
    private final TypeDescriptor<T> typeDescriptor;
    private final ImmutableList.Builder<TestSourceEvent<T>> events;
    private Instant currentWatermark;

    private Builder(TypeDescriptor<T> typeDescriptor, Optional<PipelineOptions> options) {
      this.typeDescriptor = typeDescriptor;
      events = ImmutableList.builder();

      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * Adds the specified elements to the source with timestamp equal to the current watermark.
     *
     * @return this {@link CreateStream.Builder}
     */
    @SafeVarargs
    public final Builder<T> addElements(
        T element, T... elements) {
      TimestampedValue<T> firstElement = TimestampedValue.of(element, currentWatermark);
      @SuppressWarnings("unchecked")
      TimestampedValue<T>[] remainingElements = new TimestampedValue[elements.length];
      for (int i = 0; i < elements.length; i++) {
        remainingElements[i] = TimestampedValue.of(elements[i], currentWatermark);
      }
      return addElements(firstElement, remainingElements);
    }

    /**
     * Adds the specified elements to the source with the provided timestamps.
     *
     * @return this {@link CreateStream.Builder}
     */
    @SafeVarargs
    public final Builder<T> addElements(
        TimestampedValue<T> element, TimestampedValue<T>... elements) {
      events.add(TestSourceEvent.addElements(element, elements));
      return this;
    }

    /**
     * Advance the watermark of this source to the specified instant.
     *
     * <p>The watermark must advance monotonically and to at most
     * {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
     *
     * @return this {@link CreateStream.Builder}
     */
    public Builder<T> advanceWatermarkTo(Instant newWatermark) {
      checkArgument(newWatermark.isAfter(currentWatermark),
          "The watermark must monotonically advance");
      checkArgument(newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "The Watermark cannot progress beyond the maximum. Got: %s. Maximum: %s",
          newWatermark,
          BoundedWindow.TIMESTAMP_MAX_VALUE);
      events.add(TestSourceEvent.advanceWatermarkTo(newWatermark));
      currentWatermark = newWatermark;
      return this;
    }

    /**
     * Advance the processing time by the specified amount.
     *
     * @return this {@link CreateStream.Builder}
     */
    public Builder<T> advanceProcessingTime(Duration amount) {
      checkArgument(amount.getMillis() > 0,
          "Must advance the processing time by a positive amount. Got: ",
          amount);
      events.add(TestSourceEvent.advanceProcessingTime(amount));
      return this;
    }

    /**
     * Advance the watermark to infinity, completing this {@link CreateStream}. Future calls to
     * the same builder will not affect the returned {@link CreateStream}.
     */
    public CreateStream<T> advanceWatermarkToInfinity() {
      events.add(TestSourceEvent.advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
      return new CreateStream<>(typeDescriptor, events.build());
    }
  }

  private static class TestClock implements Clock {
    private final AtomicReference<Instant> currentTime =
        new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);

    public void advance(Duration amount) {
      Instant now = currentTime.get();
      currentTime.compareAndSet(now, now.plus(amount));
    }

    @Override
    public Instant now() {
      return currentTime.get();
    }
  }


  @AutoValue
  abstract static class TestSourceEvent<T> {
    @Nullable
    public abstract Iterable<TimestampedValue<T>> getElements();

    @Nullable
    public abstract Instant getWatermark();

    @Nullable
    public abstract Duration getProcessingTimeAdvance();

    public static <T> TestSourceEvent<T> addElements(
        TimestampedValue<T> element, TimestampedValue<T>[] elements) {
      return new AutoValue_CreateStream_TestSourceEvent<>(ImmutableList
          .<TimestampedValue<T>>builder()
          .add(element)
          .add(elements)
          .build(), null, null);
    }

    public static <T> TestSourceEvent advanceWatermarkTo(Instant newWatermark) {
      return new AutoValue_CreateStream_TestSourceEvent<T>(null, newWatermark, null);
    }

    public static <T> TestSourceEvent advanceProcessingTime(Duration amount) {
      return new AutoValue_CreateStream_TestSourceEvent<T>(null, null, amount);
    }
  }

  @Override
  public PCollection<T> apply(PBegin input) {
    setup(input.getPipeline());
    return PCollection.<T>createPrimitiveOutputInternal(input.getPipeline(),
        WindowingStrategy.globalDefault(),
        IsBounded.UNBOUNDED).setTypeDescriptorInternal(typeDescriptor);
  }

  private void setup(Pipeline p) {
    PipelineOptions options = p.getOptions();
    checkState(options.getRunner() == DirectRunner.class,
        "%s can only be used when running with the %s",
        getClass().getSimpleName(),
        DirectRunner.class.getSimpleName());
    DirectOptions directOptions = options.as(DirectOptions.class);
    Clock clock = directOptions.getClock();
    if (!(clock instanceof TestClock)) {
      this.clock = new TestClock();
      directOptions.setClock(this.clock);
    } else {
      this.clock = (TestClock) clock;
    }
    directOptions.
        setTransformEvaluatorRegistryFactory(directOptions.getTransformEvaluatorRegistryFactory()
            .withAdditionalFactory(CreateStream.class, new EvaluatorFactory()));
  }

  private static class EvaluatorFactory implements TransformEvaluatorFactory {
    private final AtomicBoolean inUse = new AtomicBoolean(false);
    private AtomicReference<Evaluator<?>> evaluator = new AtomicReference<>();

    @Nullable
    @Override
    public <InputT> TransformEvaluator<InputT> forApplication(
        AppliedPTransform<?, ?, ?> application,
        @Nullable CommittedBundle<?> inputBundle,
        EvaluationContext evaluationContext) throws Exception {
      return createEvaluator((AppliedPTransform) application, evaluationContext);
    }

    private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
        AppliedPTransform<PBegin, PCollection<OutputT>, CreateStream<OutputT>> application,
        EvaluationContext evaluationContext) {
      if (evaluator.get() == null) {
        // TODO
        Evaluator<OutputT> createdEvaluator =
            new Evaluator<>(application, evaluationContext, inUse);
        evaluator.compareAndSet(null, createdEvaluator);
      }
      if (inUse.compareAndSet(false, true)) {
        return evaluator.get();
      } else {
        return null;
      }
    }
  }

  private static class Evaluator<T> implements TransformEvaluator<Object> {
    private final AppliedPTransform<PBegin, PCollection<T>, CreateStream<T>> application;
    private final EvaluationContext context;
    private final AtomicBoolean inUse;
    private final TestClock clock;
    private final List<TestSourceEvent<T>> events;
    private int index;
    private Instant currentWatermark;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, CreateStream<T>> application,
        EvaluationContext context, AtomicBoolean inUse) {
      this.application = application;
      this.context = context;
      this.inUse = inUse;
      this.clock = application.getTransform().clock;
      this.events = application.getTransform().events;
      index = 0;
      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {}

    @Override
    public TransformResult finishBundle() throws Exception {
      if (index >= events.size()) {
        return StepTransformResult.withHold(application, BoundedWindow.TIMESTAMP_MAX_VALUE).build();
      }
      TestSourceEvent<T> event = events.get(index);
      if (event.getWatermark() != null) {
        currentWatermark = event.getWatermark();
      }
      StepTransformResult.Builder result =
          StepTransformResult.withHold(application, currentWatermark);
      if (event.getElements() != null) {
        UncommittedBundle<T> bundle = context.createRootBundle(application.getOutput());
        for (TimestampedValue<T> elem : event.getElements()) {
          bundle.add(WindowedValue.timestampedValueInGlobalWindow(elem.getValue(),
              elem.getTimestamp()));
        }
        result.addOutput(bundle);
      }
      if (event.getProcessingTimeAdvance() != null) {
        clock.advance(event.getProcessingTimeAdvance());
      }
      index++;
      checkState(inUse.compareAndSet(true, false),
          "The InUse flag of a %s was changed while the source evaluator was execution. "
              + "%s cannot be split or evaluated in parallel.",
          CreateStream.class.getSimpleName(),
          CreateStream.class.getSimpleName());
      return result.build();
    }
  }
}


