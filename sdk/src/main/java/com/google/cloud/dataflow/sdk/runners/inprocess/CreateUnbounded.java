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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.DurationCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.coders.NullableCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TimestampedValue.TimestampedValueCoder;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Takes (potentially unbounded).
 */
public final class CreateUnbounded<T> extends PTransform<PBegin, PCollection<T>> {
  /**
   * Return an {@link UnfinishedCreateUnbounded} for the provided type. Elements of that type can
   * be added to the {@link UnfinishedCreateUnbounded}, and the watermark can be advanced.
   */
  public static <T> UnfinishedCreateUnbounded<T> forType(TypeDescriptor<T> td) {
    return new UnfinishedCreateUnbounded<>(td);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  private final TypeDescriptor<T> td;
  private final Optional<Coder<T>> elemCoder;
  private final List<UnboundedCreateEvent<T>> createEvents;

  private CreateUnbounded(
      TypeDescriptor<T> td,
      Optional<Coder<T>> elemCoder,
      List<UnboundedCreateEvent<T>> elementsAndWatermarks) {
    this.td = td;
    this.elemCoder = elemCoder;
    this.createEvents = elementsAndWatermarks;
  }

  @Override
  public PCollection<T> apply(PBegin begin) {
    PCollection<T> pc =
        PCollection.<T>createPrimitiveOutputInternal(
                begin.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setTypeDescriptorInternal(td);
    if (elemCoder.isPresent()) {
      pc.setCoder(elemCoder.get());
    }
    return pc;
  }

  static class CreateUnboundedEvaluatorFactory implements TransformEvaluatorFactory {
    @Override
    public <InputT> TransformEvaluator<InputT> forApplication(
        AppliedPTransform<?, ?, ?> application,
        @Nullable CommittedBundle<?> inputBundle,
        InProcessEvaluationContext evaluationContext)
        throws Exception {
      return createTransformEvaluator((AppliedPTransform) application, evaluationContext);
    }

    public <T> TransformEvaluator<?> createTransformEvaluator(
        AppliedPTransform<PBegin, PCollection<T>, CreateUnbounded<T>> application,
        InProcessEvaluationContext evaluationContext) {
      return new CreateUnboundedEvaluator<T>(application, evaluationContext);
    }
  }

  public static class CreateUnboundedEvaluator<T> implements TransformEvaluator<Object> {
    private final AppliedPTransform<PBegin, PCollection<T>, CreateUnbounded<T>> application;
    private final InProcessEvaluationContext evaluationContext;
    private int index;
    private Instant watermark;
    private Instant sleepingUntil;
    
    public CreateUnboundedEvaluator(
        AppliedPTransform<PBegin, PCollection<T>, CreateUnbounded<T>> application,
        InProcessEvaluationContext evaluationContext) {
      this.application = application;
      this.evaluationContext = evaluationContext;
      index = 0;
      watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      sleepingUntil = new Instant(Long.MIN_VALUE);
    }

    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {
      // Will never be called
    }

    @Override
    public InProcessTransformResult finishBundle() throws Exception {
      if (evaluationContext.getPipelineOptions().getClock().now().isBefore(sleepingUntil)) {
        return StepTransformResult.withHold(application, watermark).build();
      }
      UncommittedBundle<T> output = InProcessBundle.unkeyed(application.getOutput());
      List<UnboundedCreateEvent<T>> events = application.getTransform().createEvents;
      while (index < events.size()) {
        UnboundedCreateEvent<T> event = events.get(index);
        if (event.element.isPresent()) {
          TimestampedValue<T> timestampedValue = event.element.get();
          output.add(
              WindowedValue.timestampedValueInGlobalWindow(
                  event.element.get().getValue(), timestampedValue.getTimestamp()));
        } else if (event.newWatermark.isPresent()) {
          watermark = event.newWatermark.get();
          break;
        } else if (event.waitFor.isPresent()) {
          sleepingUntil =
              evaluationContext.getPipelineOptions().getClock().now().plus(event.waitFor.get());
          evaluationContext.waitingUntil(sleepingUntil);
          break;
        } else {
          throw new IllegalStateException("Had an UnboundedCreateEvent with no event");
        }
      }
      return StepTransformResult.withHold(application, watermark).addOutput(output).build();
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * A builder for {@link CreateUnbounded}.
   */
  public static final class UnfinishedCreateUnbounded<T> {
    private final TypeDescriptor<T> td;
    private final ImmutableList.Builder<UnboundedCreateEvent<T>> eventsBuilder;
    private Optional<Coder<T>> elemCoder;
    private Instant latestWatermark;

    private UnfinishedCreateUnbounded(TypeDescriptor<T> td) {
      this.td = td;
      eventsBuilder = ImmutableList.builder();
      elemCoder = Optional.absent();
      latestWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    public UnfinishedCreateUnbounded<T> withCoder(Coder<T> elemCoder) {
      this.elemCoder = Optional.of(elemCoder);
      return this;
    }

    @SafeVarargs
    public final UnfinishedCreateUnbounded<T> addElements(TimestampedValue<T>... elements) {
      for (TimestampedValue<T> element : elements) {
        eventsBuilder.add(UnboundedCreateEvent.element(element));
      }
      return this;
    }

    public UnfinishedCreateUnbounded<T> advanceWatermark(Instant newWatermark) {
      checkNotNull(newWatermark, "Cannot advance the watermark to a null value");
      checkArgument(
          !newWatermark.isBefore(latestWatermark),
          "The watermark must advance monotonically: currently %s, got %s",
          latestWatermark,
          newWatermark);
      checkArgument(
          !newWatermark.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "Cannot advance the watermark past the maximum timestamp time %s: got %s",
          BoundedWindow.TIMESTAMP_MAX_VALUE,
          newWatermark);
      eventsBuilder.add(UnboundedCreateEvent.<T>watermark(newWatermark));
      latestWatermark = newWatermark;
      return this;
    }

    public UnfinishedCreateUnbounded<T> waitFor(Duration waitTime) {
      checkNotNull(waitTime, "Cannot wait for a null argument");
      checkArgument(
          waitTime.compareTo(Duration.ZERO) > 0, "Must wait for a positive amount of time");
      eventsBuilder.add(UnboundedCreateEvent.<T>processingWait(waitTime));
      return this;
    }

    public CreateUnbounded<T> finish() {
      eventsBuilder.add(UnboundedCreateEvent.<T>watermark(BoundedWindow.TIMESTAMP_MAX_VALUE));
      return new CreateUnbounded<T>(td, elemCoder, eventsBuilder.build());
    }
  }

  private static final class UnboundedCreateEvent<T> {
    private final Optional<TimestampedValue<T>> element;
    private final Optional<Instant> newWatermark;
    private final Optional<ReadableDuration> waitFor;

    private static <T> UnboundedCreateEvent<T> element(TimestampedValue<T> element) {
      return new UnboundedCreateEvent<T>(element, null, null);
    }

    private static <T> UnboundedCreateEvent<T> watermark(Instant newWatermark) {
      return new UnboundedCreateEvent<T>(null, newWatermark, null);
    }

    private static <T> UnboundedCreateEvent<T> processingWait(ReadableDuration waitFor) {
      return new UnboundedCreateEvent<>(null, null, waitFor);
    }

    private static <T> UnboundedCreateEvent<T> of(
        TimestampedValue<T> element, Instant newWatermark, ReadableDuration waitFor) {
      return new UnboundedCreateEvent<>(element, newWatermark, waitFor);
    }

    private UnboundedCreateEvent(
        TimestampedValue<T> element, Instant newWatermark, ReadableDuration waitFor) {
      this.element = Optional.fromNullable(element);
      this.newWatermark = Optional.fromNullable(newWatermark);
      this.waitFor = Optional.fromNullable(waitFor);
    }
  }

  private static class UnboundedCreateEventCoder<T> extends StandardCoder<UnboundedCreateEvent<T>> {
    private final Coder<T> underlyingElementCoder;
    private final NullableCoder<TimestampedValue<T>> elementCoder;
    private final NullableCoder<Instant> instantCoder;
    private final NullableCoder<ReadableDuration> durationCoder;

    public static <T> UnboundedCreateEventCoder<T> of(Coder<T> elementCoder) {
      return new UnboundedCreateEventCoder<>(elementCoder);
    }

    @JsonCreator
    public static <T> UnboundedCreateEventCoder<T> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
      checkArgument(components.size() == 1, "Expecting 1 components, got " + components.size());
      @SuppressWarnings("unchecked")
      Coder<T> elementCoder = (Coder<T>) components.get(0);
      return of(elementCoder);
    }

    private UnboundedCreateEventCoder(Coder<T> elementCoder) {
      this.underlyingElementCoder = elementCoder;
      this.elementCoder = NullableCoder.of(TimestampedValueCoder.of(elementCoder));
      this.instantCoder = NullableCoder.of(InstantCoder.of());
      this.durationCoder = NullableCoder.of(DurationCoder.of());
    }

    @Override
    public void encode(UnboundedCreateEvent<T> value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      Coder.Context nestedContext = context.nested();
      elementCoder.encode(value.element.orNull(), outStream, nestedContext);
      instantCoder.encode(value.newWatermark.orNull(), outStream, nestedContext);
      durationCoder.encode(value.waitFor.orNull(), outStream, nestedContext);
    }

    @Override
    public UnboundedCreateEvent<T> decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      Coder.Context nestedContext = context.nested();
      TimestampedValue<T> value = elementCoder.decode(inStream, nestedContext);
      Instant watermark = instantCoder.decode(inStream, nestedContext);
      ReadableDuration awaitUntil = durationCoder.decode(inStream, nestedContext);
      return UnboundedCreateEvent.of(value, watermark, awaitUntil);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of(underlyingElementCoder);
    }

    public Coder<T> getElementCoder() {
      return underlyingElementCoder;
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
      elementCoder.verifyDeterministic();
      instantCoder.verifyDeterministic();
    }
  }
}

