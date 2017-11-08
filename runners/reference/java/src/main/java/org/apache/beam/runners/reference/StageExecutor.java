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
import org.apache.beam.runners.core.fn.SdkHarnessClient;
import org.apache.beam.runners.core.fn.SdkHarnessClient.ActiveBundle;
import org.apache.beam.sdk.util.WindowedValue;

/** Created by tgroh on 11/8/17. */
public class StageExecutor {
  private final ContainerManager containerManager;

  private StageExecutor(ContainerManager containerManager) {
    this.containerManager = containerManager;
  }

  public void execute(ExecutableStage stage, Iterable<WindowedValue<?>> bundle) throws Exception {
    SdkHarnessClient client = containerManager.getControlClientFor(stage.getEnvironment());
    ActiveBundle remoteBundle = client.newBundle("foo");
    Collection<OutputReceiver> outputRecievers = createReceivers(stage);
    remoteBundle.registerOutputRecievers(outputRecievers);
    for (WindowedValue<?> element : bundle) {
      remoteBundle.getInputReceiver().accept(element);
    }
  }

  private Collection<OutputReceiver> createReceivers(ExecutableStage stage) {
    throw new UnsupportedOperationException("Not yet supported");
  }
}
