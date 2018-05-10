package org.apache.beam.runners.direct.portable;

import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.local.StructuralKey;

/** A {@link StepStateAndTimers.Provider} that uses an {@link EvaluationContext}. */
class EvaluationContextStepStateAndTimersProvider implements StepStateAndTimers.Provider {
  public static StepStateAndTimers.Provider forContext(EvaluationContext context) {
    return new EvaluationContextStepStateAndTimersProvider(context);
  }

  private final EvaluationContext context;

  private EvaluationContextStepStateAndTimersProvider(EvaluationContext context) {
    this.context = context;
  }

  @Override
  public <K> StepStateAndTimers<K> forStepAndKey(PTransformNode transform, StructuralKey<K> key) {
    return context.getStateAndTimers(transform, key);
  }
}
