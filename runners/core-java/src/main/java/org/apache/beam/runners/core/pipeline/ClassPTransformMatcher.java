package org.apache.beam.runners.core.pipeline;

import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link PTransformMatcher} that matches all {@link PTransform PTransforms} that are a specific
 * class.
 */
public class ClassPTransformMatcher implements PTransformMatcher {
  public static ClassPTransformMatcher of(Class<? extends PTransform> clazz) {
    return new ClassPTransformMatcher(clazz);
  }

  private final Class<? extends PTransform> clazz;

  public ClassPTransformMatcher(Class<? extends PTransform> clazz) {
    this.clazz = clazz;
  }

  @Override
  public Match match(PTransform<?, ?> transform) {
    if (transform.getClass().equals(clazz)) {
      return Match.REPLACE;
    }
    return Match.CONTINUE;
  }
}
