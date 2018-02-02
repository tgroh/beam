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
import java.util.Collection;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/** An {@link ExecutableStage} that is created from a set of components. */
@AutoValue
abstract class ImmutableExecutableStage implements ExecutableStage {
  public static Builder builder() {
    return AutoValue_ImmutableExecutableStage.Builder();
  }

  @AutoValue.Builder static abstract class Builder {
    abstract Builder setEnvironment(Environment e);
    abstract Builder setConsumedPCollection(Optional<PCollectionNode> pcollection);
    abstract Builder setMaterializedPCollections(Collection<PCollectionNode> pcollections);
    abstract Builder setTransforms(Collection<PTransformNode> transforms);
    abstract ImmutableExecutableStage build();
  }

  @Override
  public abstract Environment getEnvironment() ;

  @Override
  public abstract Optional<PCollectionNode> getConsumedPCollection() ;

  @Override
  public abstract Collection<PCollectionNode> getMaterializedPCollections() ;

  @Override
  public abstract Collection<PTransformNode> getTransforms() ;
}
