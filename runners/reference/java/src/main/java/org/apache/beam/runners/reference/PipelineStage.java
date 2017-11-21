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

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.IdGenerator;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;

/**
 * A collection of {@link PTransform PTransforms} that can be executed as a single unit, their
 * inputs, outputs, and other associated data.
 */
public class PipelineStage {
  private final IdGenerator idGenerator;
  private final ProcessBundleDescriptor initialDescriptor;
  private final Collection<String> inputPCollectionId;
  private final Collection<String> outputPCollectionId;

  private PipelineStage(IdGenerator idGenerator, ProcessBundleDescriptor initialDescriptor) {
    this.idGenerator = idGenerator;
    this.initialDescriptor = initialDescriptor;
    // TODO: initialize for real
    inputPCollectionId = Collections.singleton("foo");
    outputPCollectionId = Collections.singleton("bar");
  }

  /**
   * Returns a {@link ProcessBundleDescriptor} containing all of the transforms in this stage, where
   * the input and output .
   *
   * @param dataEndpoint the data service that will be embedded in the RemoteGrpcPortRead and Write
   *     operations
   */
  public ProcessBundleDescriptor toProcessBundleDescriptor(
      Endpoints.ApiServiceDescriptor dataEndpoint) {
    ProcessBundleDescriptor.Builder builder =
        initialDescriptor
            .toBuilder()
            .setId(idGenerator.getId())
            .putTransforms(
                idGenerator.getId(),
                RemoteGrpcPortRead.readFromPort(
                        RemoteGrpcPort.newBuilder().setApiServiceDescriptor(dataEndpoint).build(),
                    Iterables.getOnlyElement(inputPCollectionId))
                    .toPTransform());
    for (String oPCI : outputPCollectionId) {
      RemoteGrpcPortWrite writeNode =
          RemoteGrpcPortWrite.writeToPort(
              oPCI, RemoteGrpcPort.newBuilder().setApiServiceDescriptor(dataEndpoint).build());
      builder = builder.putTransforms(idGenerator.getId(), writeNode.toPTransform());
    }
    return builder
        .build();
  }

  @Override
  public boolean equals(Object other) {
    return other == this;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }
}
