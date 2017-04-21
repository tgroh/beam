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

package org.apache.beam.runners.dataflow.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.Structs;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;

/** Utilities for creating {@link CloudObjectTranslator} instances for {@link Coder Coders}. */
public class CloudObjectTranslators {
  private CloudObjectTranslators() {}

  private static CloudObject translateStandardCoder(CloudObject base, StandardCoder<?> coder) {
    List<? extends Coder<?>> components = coder.getComponents();
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder<?> component : components) {
        cloudComponents.add(component.asCloudObject());
      }
      Structs.addList(base, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    addSharedCoderProperties(base, coder);

    return base;
  }

  private static void addSharedCoderProperties(CloudObject base, Coder<?> coder) {
    String encodingId = coder.getEncodingId();
    checkNotNull(encodingId, "Coder.getEncodingId() must not return null.");
    if (!encodingId.isEmpty()) {
      Structs.addString(base, PropertyNames.ENCODING_ID, encodingId);
    }

    Collection<String> allowedEncodings = coder.getAllowedEncodings();
    if (!allowedEncodings.isEmpty()) {
      Structs.addStringList(
          base, PropertyNames.ALLOWED_ENCODINGS, Lists.newArrayList(allowedEncodings));
    }
  }

  private static List<Coder<?>> getComponentCoders(CloudObject from) {
    List<CloudObject> components = (List<CloudObject>) from.get(PropertyNames.COMPONENT_ENCODINGS);
    List<Coder<?>> componentCoders = new ArrayList<>();
    for (CloudObject component : components) {
      Coder<?> componentCoder = CloudObjects.coderFromCloudObject(component);
      componentCoders.add(componentCoder);
    }
    return componentCoders;
  }

  private static Coder<?> getSingletonComponentCoder(CloudObject from) {
    List<Coder<?>> componentCoders = getComponentCoders(from);
    checkArgument(
        componentCoders.size() == 1,
        "Expected a single component coder for a %s, got %s",
        from.getClassName(),
        componentCoders.size());
    return componentCoders.get(0);
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "pair".
   */
  public static CloudObjectTranslator<KvCoder<?, ?>> pair() {
    return new CloudObjectTranslator<KvCoder<?, ?>>() {
      @Override
      public CloudObject toCloudObject(KvCoder<?, ?> target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_PAIR);
        Structs.addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
        return translateStandardCoder(result, target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "stream".
   */
  public static CloudObjectTranslator<IterableCoder<?>> stream() {
    return new CloudObjectTranslator<IterableCoder<?>>() {
      @Override
      public CloudObject toCloudObject(IterableCoder<?> target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_STREAM);
        Structs.addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
        return translateStandardCoder(result, target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "length_prefix".
   */
  static CloudObjectTranslator<LengthPrefixCoder<?>> lengthPrefix() {
    return new CloudObjectTranslator<LengthPrefixCoder<?>>() {
      @Override
      public CloudObject toCloudObject(LengthPrefixCoder<?> target) {
        return translateStandardCoder(
            CloudObject.forClassName(CloudObjectKinds.KIND_LENGTH_PREFIX), target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "global_window".
   */
  static CloudObjectTranslator<GlobalWindow.Coder> globalWindow() {
    return new CloudObjectTranslator<GlobalWindow.Coder>() {
      @Override
      public CloudObject toCloudObject(GlobalWindow.Coder target) {
        return translateStandardCoder(
            CloudObject.forClassName(CloudObjectKinds.KIND_GLOBAL_WINDOW), target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "interval_window".
   */
  static CloudObjectTranslator<IntervalWindowCoder> intervalWindow() {
    return new CloudObjectTranslator<IntervalWindowCoder>() {
      @Override
      public CloudObject toCloudObject(IntervalWindowCoder target) {
        return translateStandardCoder(
            CloudObject.forClassName(CloudObjectKinds.KIND_INTERVAL_WINDOW), target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "windowed_value".
   */
  static CloudObjectTranslator<FullWindowedValueCoder<?>> windowedValue() {
    return new CloudObjectTranslator<FullWindowedValueCoder<?>>() {
      @Override
      public CloudObject toCloudObject(FullWindowedValueCoder<?> target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_WINDOWED_VALUE);
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return translateStandardCoder(result, target);
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is a wrapper
   * for the target class.
   *
   * <p>Used for {@link ValueOnlyWindowedValueCoder}.
   */
  static CloudObjectTranslator<ValueOnlyWindowedValueCoder<?>> wrapper() {
    return new CloudObjectTranslator<ValueOnlyWindowedValueCoder<?>>() {
      @Override
      public CloudObject toCloudObject(ValueOnlyWindowedValueCoder<?> target) {
        CloudObject result = CloudObject.forClassName(target.getClass().getName());
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return translateStandardCoder(result, target);
      }
    };
  }
}
