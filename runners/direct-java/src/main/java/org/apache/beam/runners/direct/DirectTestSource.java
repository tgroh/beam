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
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
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
 * An Testing input that generates a stream of elements and watermark advancements.
 *
 * <p>To use, TBD.
 */
public class DirectTestSource<T> {
  private final List<TestSourceEvent<T>> events;
  private final TestClock clock;
  private final PCollection<T> outputElements;

  public static <T> Builder<T> create(TypeDescriptor<T> td) {
    return new Builder<>(td, Optional.<PipelineOptions>absent());
  }

  public static <T> Builder<T> fromOptions(TypeDescriptor<T> td, PipelineOptions options) {
    return create(td).withOptions(options);
  }

  private DirectTestSource(
      TypeDescriptor<T> typeDescriptor,
      Optional<PipelineOptions> options,
      List<TestSourceEvent<T>> events) {
    this.events = checkNotNull(events);
    PipelineOptions pipelineOptions;
    if (options.isPresent()) {
      pipelineOptions = options.get();
    } else {
      pipelineOptions = TestPipeline.testingPipelineOptions();
    }
    checkArgument(pipelineOptions.getRunner().equals(DirectRunner.class),
        "%s must use the Direct Runner to use %s",
        TestPipeline.class.getSimpleName(),
        getClass().getSimpleName());

    clock = new TestClock();
    DirectOptions directOptions = pipelineOptions.as(DirectOptions.class);
    directOptions.setClock(clock);
    directOptions.setTransformEvaluatorRegistry(directOptions.getTransformEvaluatorRegistry()
        .withEvaluatorFactory(CreateTestElements.class, new EvaluatorFactory()));

    this.outputElements = TestPipeline.fromOptions(directOptions)
        .apply(new CreateTestElements<>(clock, events))
        .setTypeDescriptorInternal(typeDescriptor);
  }

  public <OutputT extends POutput> OutputT apply(
      PTransform<? super PCollection<T>, OutputT> transform) {
    return outputElements.apply(transform);
  }

  public Pipeline getPipeline() {
    return outputElements.getPipeline();
  }

  /**
   * An incomplete {@link DirectTestSource}. Elements added to this builder will be produced in
   * sequence when the pipeline created by the {@link DirectTestSource} is run.
   */
  public static class Builder<T> {
    private final TypeDescriptor<T> typeDescriptor;
    private final ImmutableList.Builder<TestSourceEvent<T>> events;
    private Optional<PipelineOptions> options;
    private Instant currentWatermark;

    private Builder(TypeDescriptor<T> typeDescriptor, Optional<PipelineOptions> options) {
      this.typeDescriptor = typeDescriptor;
      this.options = options;
      events = ImmutableList.builder();

      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    public Builder<T> withOptions(PipelineOptions options) {
      this.options = Optional.fromNullable(options);
      return this;
    }

    @SafeVarargs
    public final Builder<T> addElements(
        TimestampedValue<T> element, TimestampedValue<T>... elements) {
      events.add(TestSourceEvent.addElements(element, elements));
      return this;
    }

    public Builder<T> advanceWatermarkTo(Instant newWatermark) {
      checkArgument(newWatermark.isAfter(currentWatermark),
          "The watermark must monotonically advance");
      checkArgument(
          newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE), "TODO: Error message");
      events.add(TestSourceEvent.advanceWatermarkTo(newWatermark));
      currentWatermark = newWatermark;
      return this;
    }

    public Builder<T> advanceProcessingTime(Duration amount) {
      events.add(TestSourceEvent.advanceProcessingTime(amount));
      return this;
    }

    public DirectTestSource<T> advanceWatermarkToInfinity() {
      events.add(TestSourceEvent.advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
      return new DirectTestSource<>(typeDescriptor, options, events.build());
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
      return new AutoValue_DirectTestSource_TestSourceEvent<T>(
          ImmutableList.<TimestampedValue<T>>builder().add(element).add(elements).build(),
          null,
          null);
    }

    public static <T> TestSourceEvent advanceWatermarkTo(Instant newWatermark) {
      return new AutoValue_DirectTestSource_TestSourceEvent<T>(null, newWatermark, null);
    }

    public static <T> TestSourceEvent advanceProcessingTime(Duration amount) {
      return new AutoValue_DirectTestSource_TestSourceEvent<T>(null, null, amount);
    }
  }

  private static class CreateTestElements<T> extends PTransform<PBegin, PCollection<T>> {
    private final TestClock clock;
    private final List<TestSourceEvent<T>> events;

    private CreateTestElements(TestClock clock, List<TestSourceEvent<T>> events) {
      this.clock = clock;
      this.events = events;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED);
    }
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
        AppliedPTransform<PBegin, PCollection<OutputT>, CreateTestElements<OutputT>> application,
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
    private final AppliedPTransform<PBegin, PCollection<T>, CreateTestElements<T>> application;
    private final EvaluationContext context;
    private final AtomicBoolean inUse;
    private final TestClock clock;
    private final List<TestSourceEvent<T>> events;
    private int index;
    private Instant currentWatermark;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, CreateTestElements<T>> application,
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
      Thread.sleep(100L);
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
          bundle.add(
              WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp()));
        }
        result.addOutput(bundle);
      }
      if (event.getProcessingTimeAdvance() != null) {
        clock.advance(event.getProcessingTimeAdvance());
      }
      index++;
      checkState(
          inUse.compareAndSet(true, false),
          "The InUse flag of a %s was changed while the source evaluator was execution. "
              + "%s cannot be split or evaluated in parallel.",
          DirectTestSource.class.getSimpleName(),
          DirectTestSource.class.getSimpleName());
      return result.build();
    }
  }
}
