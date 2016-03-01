package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.io.CountingSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Optional;

import org.joda.time.Instant;

/**
 *
 */
public class TestGenerate<T> extends PTransform<PBegin, PCollection<T>> {
  public static <T> TestGenerate<T> via(SimpleFunction<Long, T> generator) {
    return new TestGenerate<>(generator.getOutputTypeDescriptor(), generator, null);
  }

  private final TypeDescriptor<T> typeDescriptor;
  private final SerializableFunction<Long, T> generator;
  private final Optional<SerializableFunction<Long, Instant>> timestampFn;

  private TestGenerate(
      TypeDescriptor<T> td,
      SerializableFunction<Long, T> generator,
      SerializableFunction<Long, Instant> timestampFn) {
    this.typeDescriptor = td;
    this.generator = generator;
    this.timestampFn = Optional.fromNullable(timestampFn);
  }
  
  @Override
  public PCollection<T> apply(PBegin input) {
    PCollection<Long> sequence = input
        .apply(Read.from(CountingSource.unbounded()));
    if (timestampFn.isPresent()) {
      sequence = sequence.apply(WithTimestamps.of(timestampFn.get()));
    }
    return sequence.apply(MapElements.via(generator).withOutputType(typeDescriptor));
  }
}
