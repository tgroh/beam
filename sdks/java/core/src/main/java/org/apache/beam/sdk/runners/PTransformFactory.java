package org.apache.beam.sdk.runners;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/** Constructs replacement {@link PTransform PTransforms} from an original PTransform. */
@Experimental(Kind.RUNNER_API)
public interface PTransformFactory {
  /**
   * Construct a replacement {@link PTransform} from an original {@link PTransform}, or {@code null}
   * if no such transform can be constructed.
   */
  @Nullable
  <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> create(
      PTransform<InputT, OutputT> original);
}
