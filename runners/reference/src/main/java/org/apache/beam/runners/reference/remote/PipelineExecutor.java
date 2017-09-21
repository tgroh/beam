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

package org.apache.beam.runners.reference.remote;

import com.google.protobuf.Struct;
import java.io.File;
import java.io.FileInputStream;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A program that runs a Pipeline that has been serialized into a file within the process.
 */
public class PipelineExecutor {
  public static void main(String[] args) throws Exception {
    // args[0] = Pipeline file; args[1] = Pipeline Options File
    File pipelineFile = new File(args[0]);
    Pipeline pipeline =
        PipelineTranslation.fromProto(
            RunnerApi.Pipeline.parseFrom(new FileInputStream(pipelineFile)));
    PipelineOptions options =
        PipelineOptionsTranslation.fromProto(
            Struct.parseFrom(new FileInputStream(new File(args[1]))));

    runPipeline(pipeline, options);
  }

  private static void runPipeline(Pipeline pipeline, PipelineOptions options) {
    // TODO: Use an explicit runner; fiddle with options if required
    PipelineRunner runner = PipelineRunner.fromOptions(options);
    runner.run(pipeline);
  }
}
