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

import com.google.protobuf.Struct;
import java.nio.file.Path;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineTranslation;

/**
 * A {@code PipelineRunner} that executes a job via the Beam portability framework.
 */
public class ReferenceRunner {

  /** Run the provided {@link Pipeline} with the {@link ReferenceRunner}. */
  public static ReferenceRunnerJob run(Pipeline p, Struct options, Path stagingLocation)
      throws Exception {
    // Validate that the pipeline is well-formed.
    try {
      PipelineTranslation.fromProto(p);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Could not construct a Java Pipeline from the provided Pipeline", e);
    }
    /*
    docker run -v WORKER_PERSIST_DIR:SEMI_PERSIST_DIR
      <sdk-harness-container-image> \
      --id=ID \
      --logging_endpoint=LOGGING_ENDPOINT \
      --artifact_endpoint=ARTIFACT_ENDPOINT \
      --provision_endpoint=PROVISION_ENDPOINT \
      --control_endpoint=CONTROL_ENDPOINT \
      --semi_persist_dir=SEMI_PERSIST_DIR
     */
    //    ServerFactory serverFactory = ServerFactory.createDefault();
//     TODO: Perform graph surgeries, if required.
//    ExecutableGraph graph = ExecutableGraph.from(p);
    //    FnServerManager serviceManager = FnServerManager.create(serverFactory);
    //    Ctxt context = Ctxt.create(options, graph);

    // TODO: The runner is responsbile for performing the "Impulse" action
    //    RootProviderRegistry rootInputProvider = null;
    //    EvaluatorRegistry evaluatorRegistry = null;

    // TODO
    //    PipelineExecutor<PipelineStage> executor = null;
//    executor.start(graph.getRoots());
//
//    return ReferenceRunnerJob.forExecutingPipeline(executor);
    return ReferenceRunnerJob.forExecutingPipeline(null);
  }
}
