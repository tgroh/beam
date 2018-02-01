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

package org.apache.beam.runners.fnexecution.environment;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;

/**
 * A {@link RemoteEnvironment} which executes within the same process. Should be used for testing.
 */
public class InProcessEnvironment implements RemoteEnvironment {
  @Override
  public Environment getEnvironment() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SdkHarnessClient getClient() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws Exception {
    throw new UnsupportedOperationException("Not implemented");
  }
}
