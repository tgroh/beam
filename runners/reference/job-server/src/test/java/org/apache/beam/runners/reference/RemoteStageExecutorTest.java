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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RemoteStageExecutor}.
 */
@RunWith(JUnit4.class)
public class RemoteStageExecutorTest {
  @Test
  public void testStageExecution() {
    Pipeline javaPipeline = Pipeline.create();
    javaPipeline
        .apply("create", Create.of("foo", "spam"))
        .apply(
            "map",
            MapElements.via(
                new SimpleFunction<String, Integer>() {
                  @Override
                  public Integer apply(String input) {
                    return input.length();
                  }
                }));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(javaPipeline);
    // TODO: Create executable stage
    // TODO: Add the impulse element to the stage
    // TODO: Execute via direct element
  }
}
