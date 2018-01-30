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
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;

/**
 * A graph node which contains some pipeline element.
 */
interface PipelineNode {
  static PTransformNode ptransform(String id, PTransform transform) {
    return new AutoValue_PipelineNode_PTransformNode(id, transform);
  }

  static PCollectionNode pcollection(String id, PCollection collection) {
    return new AutoValue_PipelineNode_PCollectionNode(id, collection);
  }

  default PCollectionNode asPCollectionNode() {
    throw new IllegalArgumentException(
        String.format(
            "Cannot convert a %s with type %s to a %s",
            PipelineNode.class.getSimpleName(),
            getClass().getSimpleName(),
            PCollectionNode.class.getSimpleName()));
  }

  default PTransformNode asPTransformNode() {
    throw new IllegalArgumentException(
        String.format(
            "A %s with type %s is not a %s",
            PipelineNode.class.getSimpleName(),
            getClass().getSimpleName(),
            PTransformNode.class.getSimpleName()));
  }

  @AutoValue
  abstract class PCollectionNode implements PipelineNode {
    @Override
    public PCollectionNode asPCollectionNode() {
      return this;
    }

    public abstract String getId();
    public abstract PCollection getPCollection();
  }

  @AutoValue
  abstract class PTransformNode implements PipelineNode {
    @Override
    public PTransformNode asPTransformNode() {
      return this;
    }

    public abstract String getId();
    public abstract PTransform getTransform();
  }
}
