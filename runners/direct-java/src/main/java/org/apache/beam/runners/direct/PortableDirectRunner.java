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

package org.apache.beam.runners.direct;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverride;

/**
 * The "ReferenceRunner", which executes a pipeline on the local machine via use of the Portability
 * Framework.
 */
class PortableDirectRunner {
  public static PortableDirectRunner forPipeline(RunnerApi.Pipeline p) throws Exception {
    return new PortableDirectRunner(p);
  }

  private PortableDirectRunner(RunnerApi.Pipeline p) throws IOException {
    Pipeline javaP = PipelineTranslation.fromProto(p);
    javaP.replaceAll(getPortableReplacements());
    RunnerApi.Pipeline directP = PipelineTranslation.toProto(javaP);
    RunnerApi.Pipeline fusedP = GreedyPipelineFuser.fuse(directP).toPipeline();

    PortableDirectRunner runner = new PortableDirectRunner(fusedP);
  }

  private List<PTransformOverride> getPortableReplacements() {
    return ImmutableList.of();
  }

  public void execute() {}

}
