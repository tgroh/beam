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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParameterizableTransformRegistrar;
import org.apache.beam.sdk.transforms.ParameterizedPayloadFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Created by tgroh on 4/7/17. */
public class PTransforms {
  private static final Map<Class<? extends PTransform>, TransformPayloadTranslator>
      PAYLOAD_TRANSLATORS =
          ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder().build();
  // TODO: ParDoPayload, WindowIntoPayload, ReadPayload, CombinePayload
  // TODO: "Flatten Payload", etc?

  private static final Map<Class<? extends PTransform>, ParameterizedPayloadFactory<?, ?>>
      CUSTOM_REGISTERED_TRANSLATORS = getRegisteredTranslators();
  private static final String CUSTOM_TRANFORM_URN_PREFIX = "urn:beam:ptransform:0.1:custom";

  private static Map<Class<? extends PTransform>, ParameterizedPayloadFactory<?, ?>>
      getRegisteredTranslators() {
    ServiceLoader<ParameterizableTransformRegistrar> registrars =
        ServiceLoader.load(ParameterizableTransformRegistrar.class);
    ImmutableMap.Builder<Class<? extends PTransform>, ParameterizedPayloadFactory<?, ?>>
        registeredPayloadFactories = ImmutableMap.builder();
    for (ParameterizableTransformRegistrar registrar : registrars) {
      for (ParameterizedPayloadFactory registeredFactory : registrar.getFactories()) {
        registeredPayloadFactories.put(registeredFactory.getTargetClass(), registeredFactory);
      }
    }
    return registeredPayloadFactories.build();
  }

  private PTransforms() {}

  /**
   * Translates an {@link AppliedPTransform} into a runner API proto.
   *
   * <p>Does not register {@code application} within the provided {@link SdkComponents}.
   */
  public static RunnerApi.PTransform toProto(
      AppliedPTransform<?, ?, ?> application,
      List<AppliedPTransform<?, ?, ?>> subtransforms,
      SdkComponents components)
      throws IOException {
    RunnerApi.PTransform.Builder transformBuilder = RunnerApi.PTransform.newBuilder();
    for (Map.Entry<TupleTag<?>, PValue> taggedInput : application.getInputs().entrySet()) {
      checkArgument(taggedInput.getValue() instanceof PCollection);
      transformBuilder.putInputs(
          toProto(taggedInput.getKey()),
          components.registerPCollection((PCollection<?>) taggedInput.getValue()));
    }
    for (Map.Entry<TupleTag<?>, PValue> taggedOutput : application.getOutputs().entrySet()) {
      checkArgument(taggedOutput.getValue() instanceof PCollection);
      transformBuilder.putOutputs(
          toProto(taggedOutput.getKey()),
          components.registerPCollection((PCollection<?>) taggedOutput.getValue()));
    }
    for (AppliedPTransform<?, ?, ?> subtransform : subtransforms) {
      transformBuilder.addSubtransforms(components.registerPTransform(subtransform));
    }

    transformBuilder.setUniqueName(application.getFullName());
    // TODO: Display Data

    PTransform<?, ?> transform = application.getTransform();
    if (PAYLOAD_TRANSLATORS.containsKey(transform.getClass())) {
      FunctionSpec payload =
          PAYLOAD_TRANSLATORS.get(transform.getClass()).translate(application, components);
      transformBuilder.setSpec(payload);
    } else if (CUSTOM_REGISTERED_TRANSLATORS.containsKey(transform.getClass())) {
      ParameterizedPayloadFactory<?, ?> factory =
          CUSTOM_REGISTERED_TRANSLATORS.get(transform.getClass());
      toPayload(transform, factory);
    }

    return transformBuilder.build();
  }

  private static <T extends PTransform, TargetT> FunctionSpec toPayload(
      PTransform<?, ?> transform, ParameterizedPayloadFactory<T, TargetT> factory) {
    T typedTransform = (T) transform;
    byte[] payload = factory.toBytes(factory.toTarget(typedTransform));
    FunctionSpec.newBuilder()
        .setUrn(String.format("%s:%s", CUSTOM_TRANFORM_URN_PREFIX, factory.getCustomUrnPart()))
        .setParameter(
            Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(payload)).build()))
        .build();
  }

  private static String toProto(TupleTag<?> tag) {
    return tag.getId();
  }

  /**
   * A translator consumes a {@link PTransform} application and produces the appropriate
   * FunctionSpec for a distinguished or primitive transform within the Beam runner API.
   */
  interface TransformPayloadTranslator<T extends PTransform<?, ?>> {
    FunctionSpec translate(AppliedPTransform<?, ?, T> transform, SdkComponents components);
  }
}
