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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.core.construction.JobNotFoundException;
import org.apache.beam.runners.core.construction.PipelineManager;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A {@link PipelineManager} for the {@link DirectRunner}.
 */
class DirectPipelineManager implements PipelineManager {
  public static DirectPipelineManager create() {
    return new DirectPipelineManager();
  }

  private final ConcurrentMap<String, DirectPipelineResult> results;

  private DirectPipelineManager() {
    results = new ConcurrentHashMap<>();
  }

  @Override
  public String run(String jobName, Pipeline pipeline, PipelineOptions options) {
    String jobId = String.format("direct-%s-%s", System.nanoTime(), jobName);
    results.put(jobId, DirectRunner.fromOptions(options).run(pipeline));
    return jobId;
  }

  @Override
  public State getState(String jobId) throws JobNotFoundException {
    return getResult(jobId).getState();
  }

  @Override
  public State cancel(String jobId) throws JobNotFoundException {
    return getResult(jobId).getState();
  }

  @Override
  public Iterator<StateOrMessage> getUpdateStream(String jobId) throws JobNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private DirectPipelineResult getResult(String jobId) throws JobNotFoundException {
    DirectPipelineResult result = results.get(jobId);
    if (result == null) {
      throw new JobNotFoundException(String.format("No Job with ID %s", jobId));
    }
    return result;
  }
}
