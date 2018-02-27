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

package org.apache.beam.runners.core.construction.graph;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components.Builder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/** A {@link Pipeline} which has been separated into collections of executable components. */
@AutoValue
public abstract class FusedPipeline {
  static FusedPipeline of(
      Set<ExecutableStage> environmentalStages, Set<PTransformNode> runnerStages) {
    return new AutoValue_FusedPipeline(environmentalStages, runnerStages);
  }

  /** The {@link ExecutableStage executable stages} that are executed by SDK harnesses. */
  public abstract Set<ExecutableStage> getFusedStages();

  /** The {@link PTransform PTransforms} that a runner is responsible for executing. */
  public abstract Set<PTransformNode> getRunnerExecutedTransforms();

  public RunnerApi.Pipeline toPipeline(Components initialComponents) {
    Components pipelineComponents = populateComponents(initialComponents);
    List<String> rootTransformIds = new ArrayList<>();
    for (Entry<String, PTransform> transformEntry :
        pipelineComponents.getTransformsMap().entrySet()) {
      if (getRunnerExecutedTransforms()
              .contains(PipelineNode.pTransform(transformEntry.getKey(), transformEntry.getValue()))
          || ExecutableStage.URN.equals(transformEntry.getValue().getSpec().getUrn())) {
        // This is a runner-executed transform or an ExecutableStage, which are the two types of
        // top-level transforms in the fused representation.
        // TODO: addParents(transformEntry.getValue(), pipelineComponents);
      }
      // Otherwise, this is a SDK-executed transform, and a subtransform of an ExecutableStage
    }
    return Pipeline.newBuilder()
        .setComponents(pipelineComponents)
        .addAllRootTransformIds(rootTransformIds)
        .build();
  }

  /**
   * Return a {@link Components} like the {@code base} components, but with the only transforms
   * equal to this fused pipeline.
   *
   * <p>The only composites will be the stages returned by {@link #getFusedStages()}.
   */
  private Components populateComponents(Components base) {
    Builder newComponents = base.toBuilder().clearTransforms();
    for (PTransformNode runnerExecuted : getRunnerExecutedTransforms()) {
      newComponents.putTransforms(runnerExecuted.getId(), runnerExecuted.getTransform());
    }
    for (ExecutableStage stage : getFusedStages()) {
      for (PTransformNode fusedTransform : stage.getTransforms()) {
        newComponents.putTransforms(fusedTransform.getId(), fusedTransform.getTransform());
      }
      newComponents.putTransforms(stageId(stage, newComponents), stage.toPTransform());
    }
    return newComponents.build();
  }

  private String stageId(ExecutableStage stage, Components.Builder cbuilder) {
    int i = 0;
    String name;
    do {
      // Instead this could include the name of the root transforms
      name =
          String.format(
              "%s/%s.%s",
              stage.getInputPCollection().getPCollection().getUniqueName(),
              stage.getEnvironment().getUrl(),
              i);
      i++;
    } while (cbuilder.containsTransforms(name));
    return name;
  }
}
