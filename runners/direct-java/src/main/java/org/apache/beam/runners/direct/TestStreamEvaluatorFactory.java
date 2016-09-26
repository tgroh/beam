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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.Event;
import org.apache.beam.sdk.testing.TestStream.EventType;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** The {@link TransformEvaluatorFactory} for the {@link TestStream} primitive. */
class TestStreamEvaluatorFactory implements RootTransformEvaluatorFactory {
  private final KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> evaluators =
      LockedKeyedResourcePool.create();
  private final EvaluationContext evaluationContext;

  TestStreamEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  /**
   * {@inheritDoc}
   *
   * @return an empty list. {@link TestStream} depends on the quiescence properties of the pipeline
   * to be invoked and produce output.
   */
  @Override
  public List<CommittedBundle<?>> getInitialInputs(AppliedPTransform<?, ?, ?> transform) {
    return getStartIndex((AppliedPTransform) transform);
  }

  private <T> List<CommittedBundle<TestStreamIndex<T>>> getStartIndex(
      AppliedPTransform<?, ?, TestStream<T>> transform) {
    // TODO: Make this consistent with other reads
    WindowedValue<TestStreamIndex<T>> startIndex =
        WindowedValue.valueInGlobalWindow(TestStreamIndex.of(transform.getTransform(), 0));
    UncommittedBundle<TestStreamIndex<T>> bundle = evaluationContext.createRootBundle();
    bundle.add(startIndex);
    return Collections.singletonList(bundle.commit(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle)
      throws Exception {
    return createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
  }

  @Override
  public void cleanup() throws Exception {}

  /**
   * Returns the evaluator for the provided application of {@link TestStream}, or null if it is
   * already in use.
   *
   * <p>The documented behavior of {@link TestStream} requires the output of one event to travel
   * completely through the pipeline before any additional event, so additional instances that have
   * a separate collection of events cannot be created.
   */
  private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application,
      CommittedBundle<TestStreamIndex<OutputT>> inputBundle)
      throws ExecutionException {
    checkArgument(
        Iterables.size(inputBundle.getElements()) == 1,
        "Should never get an evaluator for multiple Test Stream indicies at a time");
    return (TransformEvaluator<? super InputT>)
        new Evaluator<>(application, inputBundle, evaluationContext);
  }

  private static class Evaluator<T> implements TransformEvaluator<TestStreamIndex<T>> {
    private final AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application;
    private final CommittedBundle<TestStreamIndex<T>> input;
    private final EvaluationContext context;
    private final StepTransformResult.Builder resultBuilder;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application,
        CommittedBundle<TestStreamIndex<T>> input,
        EvaluationContext context) {
      this.application = application;
      this.input = input;
      this.context = context;
      this.resultBuilder = StepTransformResult.withoutHold(application);
    }

    @Override
    public void processElement(WindowedValue<TestStreamIndex<T>> element) throws Exception {
      TestStreamIndex<T> value = element.getValue();
      List<Event<T>> events = value.getStream().getEvents();
      int index = value.getIndex();
      if (index >= events.size()) {
        // Nothing to do.
        return;
      }
      Event<T> event = events.get(index);

      TestStreamIndex<T> nextIndex = TestStreamIndex.of(value.getStream(), value.getIndex() + 1);
      if (event.getType().equals(EventType.WATERMARK)) {
        Instant newWatermark = ((WatermarkEvent<T>) event).getWatermark();
        WindowedValue<TestStreamIndex<T>> next =
            WindowedValue.timestampedValueInGlobalWindow(nextIndex, newWatermark);
        resultBuilder.addUnprocessedElements(
            Collections.singleton(next));
      } else {
        resultBuilder.addUnprocessedElements(Collections.singleton(element.withValue(nextIndex)));
      }

      StepTransformResult.Builder result =
          StepTransformResult.withoutHold(application);
      if (event.getType().equals(EventType.ELEMENT)) {
        UncommittedBundle<T> bundle = context.createBundle(input, application.getOutput());
        for (TimestampedValue<T> elem : ((ElementEvent<T>) event).getElements()) {
          bundle.add(
              WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp()));
        }
        result.addOutput(bundle);
      }
      if (event.getType().equals(EventType.PROCESSING_TIME)) {
        ((TestClock) context.getClock())
            .advance(((ProcessingTimeEvent<T>) event).getProcessingTimeAdvance());
      }
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      return resultBuilder.build();
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

  private static class TestClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return new TestClock();
    }
  }

  static class DirectTestStreamFactory implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
        PTransform<InputT, OutputT> transform) {
      if (transform instanceof TestStream) {
        return (PTransform<InputT, OutputT>)
            new DirectTestStream<OutputT>((TestStream<OutputT>) transform);
      }
      return transform;
    }

    private static class DirectTestStream<T> extends PTransform<PBegin, PCollection<T>> {
      private final TestStream<T> original;

      private DirectTestStream(TestStream transform) {
        this.original = transform;
      }

      @Override
      public PCollection<T> apply(PBegin input) {
        PipelineRunner runner = input.getPipeline().getRunner();
        checkState(
            runner instanceof DirectRunner,
            "%s can only be used when running with the %s",
            getClass().getSimpleName(),
            DirectRunner.class.getSimpleName());
        ((DirectRunner) runner).setClockSupplier(new TestClockSupplier());
        return PCollection.<T>createPrimitiveOutputInternal(
                input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(original.getValueCoder());
      }
    }
  }

  @AutoValue
  abstract static class TestStreamIndex<T> {
    public static <T> TestStreamIndex<T> of(TestStream<T> stream, int index) {
      return new AutoValue_TestStreamEvaluatorFactory_TestStreamIndex<>(stream, index);
    }
    abstract TestStream<T> getStream();

    abstract int getIndex();
  }
}
