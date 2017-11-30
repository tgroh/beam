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

package org.apache.beam.runners.reference;

import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;

/**
 * A graph that contains all of the information required to be executable by the {@link
 * ReferenceRunner}.
 */
public class ExecutableGraph {
  public static ExecutableGraph from(Pipeline p) {
    return new ExecutableGraph(p, new NoOpFuser());
  }

  private ExecutableGraph(Pipeline p, Fuser fuser) {
    // TODO: Fusion, Other stuff
  }

  public Collection<PipelineStage> getRoots() {
    throw new UnsupportedOperationException("TODO: Implement");
  }

  public Collection<PipelineStage> getPerElementConsumers(PCollection pcollection) {
    throw new UnsupportedOperationException("TODO: Implement");
  }

  interface Fuser {}

  static class NoOpFuser implements Fuser {}

  static class ProducerConsumerSameEnvironmentFuser {}
}
