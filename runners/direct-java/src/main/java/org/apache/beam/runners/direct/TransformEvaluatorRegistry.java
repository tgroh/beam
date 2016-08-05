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

import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten.FlattenPCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
public class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  public static TransformEvaluatorRegistry defaultRegistry() {
    @SuppressWarnings("rawtypes")
    ImmutableMap<Class<? extends PTransform>, TransformEvaluatorFactory> primitives =
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .put(Read.Bounded.class, new BoundedReadEvaluatorFactory())
            .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory())
            .put(ParDo.Bound.class, new ParDoSingleEvaluatorFactory())
            .put(ParDo.BoundMulti.class, new ParDoMultiEvaluatorFactory())
            .put(FlattenPCollectionList.class, new FlattenEvaluatorFactory())
            .put(ViewEvaluatorFactory.WriteView.class, new ViewEvaluatorFactory())
            .put(Window.Bound.class, new WindowEvaluatorFactory())
            // Runner-specific primitives used in expansion of GroupByKey
            .put(DirectGroupByKeyOnly.class, new GroupByKeyOnlyEvaluatorFactory())
            .put(DirectGroupAlsoByWindow.class, new GroupAlsoByWindowEvaluatorFactory())
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories;

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext)
      throws Exception {
    TransformEvaluatorFactory factory = factories.get(application.getTransform().getClass());
    return factory.forApplication(application, inputBundle, evaluationContext);
  }

  public TransformEvaluatorRegistry withEvaluatorFactory(
      Class<? extends PTransform> transformClass, TransformEvaluatorFactory evaluatorFactory) {
    return new TransformEvaluatorRegistry(
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .putAll(factories)
            .put(transformClass, evaluatorFactory)
            .build());
  }

  static class DefaultFactory implements DefaultValueFactory<Factory> {
    @Override
    public Factory create(PipelineOptions options) {
      return new Factory(
          Collections.<Class<? extends PTransform>, TransformEvaluatorFactory>emptyMap());
    }
  }


  /**
   * A factory to create Transform Evaluator Registires.
   */
  public static class Factory {
    private final Map<Class<? extends PTransform>, TransformEvaluatorFactory> additionalFactories;

    private Factory(
        Map<Class<? extends PTransform>, TransformEvaluatorFactory> additionalFactories) {
      this.additionalFactories = additionalFactories;
    }

    public TransformEvaluatorRegistry create() {
      TransformEvaluatorRegistry registry =
          TransformEvaluatorRegistry.defaultRegistry();
      for (Map.Entry<Class<? extends PTransform>, TransformEvaluatorFactory> factory
          : additionalFactories.entrySet()) {
        registry = registry.withEvaluatorFactory(factory.getKey(), factory.getValue());
      }
      return registry;
    }

    public Factory withAdditionalFactory(
        Class<? extends PTransform> clazz,
        TransformEvaluatorFactory factory) {
      return new Factory(
          ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
          .putAll(additionalFactories)
          .put(clazz, factory)
          .build());
    }
  }
}
