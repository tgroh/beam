package org.apache.beam.runners.direct;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;

import com.google.auto.value.AutoValue;

/**
 * Tracks work that is pending but has not begun processing.
 *
 * <p>Implementations of {@link WorkManager} are required to be thread-safe.
 */
interface WorkManager {
  @AutoValue
  abstract class Work {
    public abstract AppliedPTransform<?, ?, ?> getTransform();
    public abstract CommittedBundle<?> getBundle();

    public static Work of(AppliedPTransform<?, ?, ?> transform, CommittedBundle<?> input) {
      return new AutoValue_WorkManager_Work(transform, input);
    }
  }

  void addWork(
      AppliedPTransform<?, ?, ?> transform,
      CommittedBundle<?> bundle);

  void returnWork(
      AppliedPTransform<?, ?, ?> transform,
      CommittedBundle<?> bundle);

  Iterable<Work> getWork();
}
