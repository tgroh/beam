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

package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.ParDoTranslation.translateTimerSpec;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getStateSpecOrThrow;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getTimerSpecOrThrow;

import com.google.auto.service.AutoService;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.DisplayData;
import org.apache.beam.runners.core.construction.ForwardingPTransform;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} that produces {@link ParDoSingle} instances from {@link
 * ParDo.SingleOutput} instances. {@link ParDoSingle} is a primitive {@link PTransform}, to ensure
 * that {@link DisplayData} appears on all {@link ParDo ParDos} in the {@link DataflowRunner}.
 */
public class PrimitiveParDoSingleFactory<InputT, OutputT>
    extends SingleInputOutputOverrideFactory<
        PCollection<? extends InputT>, PCollection<OutputT>, ParDo.SingleOutput<InputT, OutputT>> {
  @Override
  public PTransformReplacement<PCollection<? extends InputT>, PCollection<OutputT>>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<? extends InputT>, PCollection<OutputT>,
                  SingleOutput<InputT, OutputT>>
              transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new ParDoSingle<>(
            transform.getTransform(),
            Iterables.getOnlyElement(transform.getOutputs().keySet()),
            PTransformReplacements.getSingletonMainOutput(transform).getCoder()));
  }

  /** A single-output primitive {@link ParDo}. */
  public static class ParDoSingle<InputT, OutputT>
      extends ForwardingPTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    private final ParDo.SingleOutput<InputT, OutputT> original;
    private final TupleTag<?> onlyOutputTag;
    private final Coder<OutputT> outputCoder;

    private ParDoSingle(
        SingleOutput<InputT, OutputT> original,
        TupleTag<?> onlyOutputTag,
        Coder<OutputT> outputCoder) {
      this.original = original;
      this.onlyOutputTag = onlyOutputTag;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), outputCoder);
    }

    public DoFn<InputT, OutputT> getFn() {
      return original.getFn();
    }

    public TupleTag<?> getMainOutputTag() {
      return onlyOutputTag;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return original.getSideInputs();
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(getSideInputs());
    }

    @Override
    protected PTransform<PCollection<? extends InputT>, PCollection<OutputT>> delegate() {
      return original;
    }
  }

  /** A translator for {@link ParDoSingle}. */
  public static class PayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<ParDoSingle<?, ?>> {
    public static PTransformTranslation.TransformPayloadTranslator create() {
      return new PayloadTranslator();
    }

    private PayloadTranslator() {}

    @Override
    public String getUrn(ParDoSingle<?, ?> transform) {
      return PTransformTranslation.PAR_DO_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, ParDoSingle<?, ?>> transform, SdkComponents components)
        throws IOException {
      RunnerApi.ParDoPayload payload = payloadForParDoSingle(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PAR_DO_TRANSFORM_URN)
          .setPayload(payload.toByteString())
          .build();
    }

    @Override
    public PTransformTranslation.RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      throw new UnsupportedOperationException(
          String.format(
              "%s.rehydrate should never be called; the serialized form is that of a ParDo",
              getClass().getCanonicalName()));
    }

    private static RunnerApi.ParDoPayload payloadForParDoSingle(
        final ParDoSingle<?, ?> parDo, SdkComponents components)
        throws IOException {
      final DoFn<?, ?> doFn = parDo.getFn();
      final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
      checkArgument(
          !signature.processElement().isSplittable(),
          String.format(
              "Not expecting a splittable %s: should have been overridden",
              ParDoSingle.class.getSimpleName()));

      return ParDoTranslation.payloadForParDoLike(
          new ParDoTranslation.ParDoLike() {
            @Override
            public RunnerApi.SdkFunctionSpec translateDoFn(SdkComponents newComponents) {
              return ParDoTranslation.translateDoFn(
                  parDo.getFn(), parDo.getMainOutputTag(), newComponents);
            }

            @Override
            public List<RunnerApi.Parameter> translateParameters() {
              return ParDoTranslation.translateParameters(
                  signature.processElement().extraParameters());
            }

            @Override
            public Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components) {
              return ParDoTranslation.translateSideInputs(parDo.getSideInputs(), components);
            }

            @Override
            public Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
                throws IOException {
              Map<String, RunnerApi.StateSpec> stateSpecs = new HashMap<>();
              for (Map.Entry<String, DoFnSignature.StateDeclaration> state :
                  signature.stateDeclarations().entrySet()) {
                RunnerApi.StateSpec spec =
                    ParDoTranslation.translateStateSpec(
                        getStateSpecOrThrow(state.getValue(), doFn), components);
                stateSpecs.put(state.getKey(), spec);
              }
              return stateSpecs;
            }

            @Override
            public Map<String, RunnerApi.TimerSpec> translateTimerSpecs(
                SdkComponents newComponents) {
              Map<String, RunnerApi.TimerSpec> timerSpecs = new HashMap<>();
              for (Map.Entry<String, DoFnSignature.TimerDeclaration> timer :
                  signature.timerDeclarations().entrySet()) {
                RunnerApi.TimerSpec spec =
                    translateTimerSpec(getTimerSpecOrThrow(timer.getValue(), doFn));
                timerSpecs.put(timer.getKey(), spec);
              }
              return timerSpecs;
            }

            @Override
            public boolean isSplittable() {
              return false;
            }

            @Override
            public String translateRestrictionCoderId(SdkComponents newComponents) {
              return "";
            }
          },
          components);
    }
  }

  /** Registers {@link PayloadTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(ParDoSingle.class, new PayloadTranslator());
    }

    @Override
    public Map<String, ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformRehydrators() {
      return Collections.emptyMap();
    }
  }
}
