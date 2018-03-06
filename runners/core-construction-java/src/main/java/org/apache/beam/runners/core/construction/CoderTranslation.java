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

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiFunction;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.SerializableUtils;

/** Converts to and from Beam Runner API representations of {@link Coder Coders}. */
public class CoderTranslation {
  // This URN says that the coder is just a UDF blob this SDK understands
  // TODO: standardize such things
  public static final String JAVA_SERIALIZED_CODER_URN = "urn:beam:coders:javasdk:0.1";

  @VisibleForTesting
  static final BiMap<Class<? extends Coder>, String> KNOWN_CODER_URNS = loadCoderURNs();

  @VisibleForTesting
  static final Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> KNOWN_TRANSLATORS =
      loadTranslators();

  private static BiMap<Class<? extends Coder>, String> loadCoderURNs() {
    ImmutableBiMap.Builder<Class<? extends Coder>, String> coderUrns = ImmutableBiMap.builder();
    for (CoderTranslatorRegistrar registrar : ServiceLoader.load(CoderTranslatorRegistrar.class)) {
      coderUrns.putAll(registrar.getCoderURNs());
    }
    return coderUrns.build();
  }

  private static Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> loadTranslators() {
    ImmutableMap.Builder<Class<? extends Coder>, CoderTranslator<? extends Coder>> translators =
        ImmutableMap.builder();
    for (CoderTranslatorRegistrar coderTranslatorRegistrar :
        ServiceLoader.load(CoderTranslatorRegistrar.class)) {
      translators.putAll(coderTranslatorRegistrar.getCoderTranslators());
    }
    return translators.build();
  }

  public static RunnerApi.MessageWithComponents toProto(Coder<?> coder) throws IOException {
    SdkComponents components = SdkComponents.create();
    RunnerApi.Coder coderProto = toProto(coder, components);
    return RunnerApi.MessageWithComponents.newBuilder()
        .setCoder(coderProto)
        .setComponents(components.toComponents())
        .build();
  }

  public static RunnerApi.Coder toProto(Coder<?> coder, SdkComponents components)
      throws IOException {
    if (KNOWN_TRANSLATORS.containsKey(coder.getClass())) {
      return toKnownCoder(coder, components);
    }
    return toCustomJavaCoder(coder);
  }

  private static RunnerApi.Coder toKnownCoder(Coder<?> coder, SdkComponents components)
      throws IOException {
    CoderTranslator translator = KNOWN_TRANSLATORS.get(coder.getClass());
    List<String> componentIds = registerComponents(coder, translator, components);
    return RunnerApi.Coder.newBuilder()
        .addAllComponentCoderIds(componentIds)
        .setSpec(
            SdkFunctionSpec.newBuilder()
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(translator.getUrn(coder))
                        .setPayload(ByteString.copyFrom(translator.getPayload(coder)))))
        .build();
  }

  private static <T extends Coder<?>> List<String> registerComponents(
      T coder, CoderTranslator<T> translator, SdkComponents components) throws IOException {
    List<String> componentIds = new ArrayList<>();
    for (Coder<?> component : translator.getComponents(coder)) {
      componentIds.add(components.registerCoder(component));
    }
    return componentIds;
  }

  private static RunnerApi.Coder toCustomJavaCoder(Coder<?> coder) {
    RunnerApi.Coder.Builder coderBuilder = RunnerApi.Coder.newBuilder();
    return coderBuilder
        .setSpec(
            SdkFunctionSpec.newBuilder()
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(JAVA_SERIALIZED_CODER_URN)
                        .setPayload(
                            ByteString.copyFrom(SerializableUtils.serializeToByteArray(coder)))
                        .build()))
        .build();
  }

  public static Coder<?> fromProto(RunnerApi.Coder protoCoder, RehydratedComponents components)
      throws IOException {
    return fromKnownProtoOrDefault(protoCoder, components, (coders, bytes) -> fromCustomCoder(bytes));
  }

  private static Coder<?> fromKnownProtoOrDefault(
      RunnerApi.Coder coder,
      RehydratedComponents components,
      BiFunction<List<? extends Coder<?>>, byte[], Coder<?>> unknownHandler)
      throws IOException {
    String coderUrn = coder.getSpec().getSpec().getUrn();
    List<Coder<?>> coderComponents = new LinkedList<>();
    for (String componentId : coder.getComponentCoderIdsList()) {
      Coder<?> innerCoder = components.getCoder(componentId);
      coderComponents.add(innerCoder);
    }
    Class<? extends Coder> coderType = KNOWN_CODER_URNS.inverse().get(coderUrn);
    CoderTranslator<?> translator = KNOWN_TRANSLATORS.get(coderType);
    if (translator == null) {
      return unknownHandler.apply(coderComponents, coder.getSpec().getSpec().getPayload().toByteArray());
    }
    return translator.fromComponents(
        coderComponents, coder.getSpec().getSpec().getPayload().toByteArray());
  }

  private static Coder<?> fromCustomCoder(RunnerApi.Coder coder, ) {
    return (Coder<?>) SerializableUtils.deserializeFromByteArray(payload, "Custom Coder Bytes");
  }

  private static class RawCoder<T> extends Coder<T> {
    private final FunctionSpec spec;
    private final List<Coder<?>> components;

    private RawCoder(FunctionSpec spec, List<Coder<?>> components) {
      this.spec = spec;
      this.components = components;
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
      throw new UnsupportedOperationException(
          String.format("Can't encode with a %s", RawCoder.class.getSimpleName()));
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
      throw new UnsupportedOperationException(
          String.format("Can't decode with a %s", RawCoder.class.getSimpleName()));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      throw new UnsupportedOperationException(
          String.format("Can't getCoderArguments with a %s", RawCoder.class.getSimpleName()));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new UnsupportedOperationException(
          String.format("Can't verifyDeterministic with a %s", RawCoder.class.getSimpleName()));
    }

    public List<Coder<?>> getComponents() {
      return components;
    }

    public FunctionSpec getSpec() {
      return spec;
    }
  }

  private static class RawCoderTranslator implements CoderTranslator<RawCoder<?>> {
    @Override
    public String getUrn(RawCoder<?> from) {
      return from.getSpec().getUrn();
    }

    @Override
    public List<? extends Coder<?>> getComponents(RawCoder<?> from) {
      return from.getComponents();
    }

    @Override
    public byte[] getPayload(RawCoder<?> from) {
      return from.getSpec().getPayload().toByteArray();
    }

    @Override
    public RawCoder<?> fromComponents(List<Coder<?>> components, byte[] payload) {
      throw new UnsupportedOperationException(
          String.format(
              "Can't use fromComponents to construct a %s because the URN is not available",
              RawCoder.class.getSimpleName()));
    }
  }

  @AutoService(CoderTranslatorRegistrar.class)
  static class RawCoderRegistrar implements CoderTranslatorRegistrar {
    @Override
    public Map<Class<? extends Coder>, String> getCoderURNs() {
      // No fixed URN for RawCoder, so this translator isn't symmetric
      return Collections.emptyMap();
    }

    @Override
    public Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getCoderTranslators() {
      return Collections.singletonMap(RawCoder.class, new RawCoderTranslator());
    }
  }
}
