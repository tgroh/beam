package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * An unbounded data generator.
 */
public class Generate<T> extends PTransform<PBegin, PCollection<T>> {
  public static <T> Generate<T> create(SimpleFunction<Long, T> sequenceTranslator) {
    return new Generate<>(sequenceTranslator);
  }

  private final SimpleFunction<Long, T> sequenceTranslator;

  private Generate(SimpleFunction<Long, T> sequenceTranslator) {
    this.sequenceTranslator = sequenceTranslator;
  }

  @Override
  public PCollection<T> apply(PBegin input) {
    return input
        .apply(Read.from(CountingSource.unbounded()))
        .apply(MapElements.<Long, T>via(sequenceTranslator));
  }
}
