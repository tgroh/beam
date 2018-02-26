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

package org.apache.beam.runners.core.construction.graph;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/** A {@link TransformPayloadTranslator} for the {@link ExecutableStage} intermediate transform. */
public class ExecutableStageTranslator {
  public static ExecutableStage fromTransform(AppliedPTransform<?, ?, ?> transform) {
    checkArgument(
        PTransformTranslation.urnForTransform(transform.getTransform()).equals(ExecutableStage.URN),
        "Should only be used for an transform with URN %s",
        ExecutableStage.URN);
  }

  /** A {@link PTransform} which contains an {@link ExecutableStage}. */
  public static class ExecutableStageLike<InT extends PInput, OuT extends POutput>
      extends RawPTransform<InT, OuT> {
    private final ExecutableStage executableStage;

    private ExecutableStageLike(ExecutableStage executableStage) {
      this.executableStage = executableStage;
    }

    public ExecutableStage getStage() {
      return executableStage;
    }

    @Nullable
    @Override
    public FunctionSpec getSpec() {
      return FunctionSpec.newBuilder().setUrn(ExecutableStage.URN).build();
    }
  }

  private static class Translator<InT extends PInput, OutT extends POutput>
      implements TransformPayloadTranslator<PTransform<InT, OutT>> {

    @Override
    public String getUrn(PTransform<InT, OutT> transform) {
      return ExecutableStage.URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, PTransform<InT, OutT>> application, SdkComponents components) {
      return FunctionSpec.newBuilder().setUrn(ExecutableStage.URN).build();
    }

    @Override
    public RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents) {
      ExecutableStage executableStage =
          ExecutableStage.fromPTransform(protoTransform, rehydratedComponents.toComponents());
      return new ExecutableStageLike<>(executableStage);
    }
  }

  /** The Registrar for {@link ExecutableStageTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ExecutableStageTranslatorRegistrar
      implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, ? extends TransformPayloadTranslator> getTransformRehydrators() {
      return Collections.singletonMap(ExecutableStage.URN, new Translator());
    }
  }
}
