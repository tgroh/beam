package org.apache.beam.runners.direct;

import org.apache.beam.sdk.runners.TransformFilter;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Created by tgroh on 11/16/16.
 */
class ClassTransformFilter implements TransformFilter {
  public static ClassTransformFilter of(Class<? extends PTransform> clazz) {
    return new ClassTransformFilter(clazz);
  }

  private final Class<? extends PTransform> clazz;

  private ClassTransformFilter(Class<? extends PTransform> clazz) {
    this.clazz = clazz;
  }
}
