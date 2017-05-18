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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.CombinePayload;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.SerializableUtils;

/**
 * Methods for translating between {@link Combine.PerKey} {@link PTransform PTransforms} and {@link
 * RunnerApi.CombinePayload} protos.
 */
public class CombineTranslator {
  public static CombinePayload toProto(
      AppliedPTransform<?, ?, Combine.PerKey<?, ?, ?>> combine, SdkComponents sdkComponents)
      throws IOException {
    Coder<?> accumulatorCoder = null;
    Map<String, SideInput> sideInputs = new HashMap<>();
    return RunnerApi.CombinePayload.newBuilder()
        .setAccumulatorCoderId(sdkComponents.registerCoder(accumulatorCoder))
        .putAllSideInputs(sideInputs)
        .setCombineFn(toProto(combine.getTransform().getFn()))
        .build();
  }

  private static SdkFunctionSpec toProto(GlobalCombineFn<?, ?, ?> combineFn) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(RunnerApiUrns.JAVA_SDK_URN)
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(RunnerApiUrns.JAVA_SERIALIZED_COMBINE_FN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(combineFn)))
                            .build())))
        .build();
  }

  public static Coder<?> getAccumulatorCoder(CombinePayload payload, RunnerApi.Components components)
      throws IOException {
    String id = payload.getAccumulatorCoderId();
    return Coders.fromProto(components.getCodersOrThrow(id), components);
  }

  public static GlobalCombineFn<?, ?, ?> getCombineFn(CombinePayload payload)
      throws IOException {
    checkArgument(payload.getCombineFn().getEnvironmentId().equals(RunnerApiUrns.JAVA_SDK_URN));
    return (GlobalCombineFn<?, ?, ?>)
        SerializableUtils.deserializeFromByteArray(
            payload
                .getCombineFn()
                .getSpec()
                .getParameter()
                .unpack(BytesValue.class)
                .getValue()
                .toByteArray(),
            "CombineFn");
  }
}
