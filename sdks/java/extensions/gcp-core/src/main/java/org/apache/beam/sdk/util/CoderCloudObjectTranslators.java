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

package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;

/**
 * Utilities for creating {@link CloudObjectTranslator} instances for {@link Coder Coders}.
 */
public class CoderCloudObjectTranslators {

  private CoderCloudObjectTranslators() {}

  public static CloudObject forCoderClass(Coder<?> coder) {
    return CloudObject.forClass(coder.getClass());
  }

  public static CloudObject forCustomCoder(CustomCoder<?> customCoder) {
    // N.B. We use the CustomCoder class, not the derived class, since during
    // deserialization we will be using the CustomCoder's static factory method
    // to construct an instance of the derived class.
    CloudObject result = CloudObject.forClass(CustomCoder.class);
    Structs.addString(result, "type", customCoder.getClass().getName());
    Structs.addString(
        result,
        "serialized_coder",
        StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(customCoder)));
    return result;
  }

  private static CloudObject translateStandardCoder(
      CloudObject base, StandardCoder<?> coder, List<Coder<?>> componentCoders) {
    if (!componentCoders.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(componentCoders.size());
      for (Coder<?> component : componentCoders) {
        cloudComponents.add(component.asCloudObject());
      }
      Structs.addList(base, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

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

    return base;
  }

  private static List<Coder<?>> getComponentCoders(CloudObject from) {
    List<CloudObject> components = (List<CloudObject>) from.get(PropertyNames.COMPONENT_ENCODINGS);
    List<Coder<?>> componentCoders = new ArrayList<>();
    for (CloudObject component : components) {
      Coder<?> componentCoder = CloudObjects.fromCloudObject(component);
      componentCoders.add(componentCoder);
    }
    return componentCoders;
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "pair".
   */
  public static CloudObjectTranslator<KvCoder<?, ?>> pair() {
    return new CloudObjectTranslator<KvCoder<?, ?>>() {
      @Override
      public CloudObject toCloudObject(KvCoder<?, ?> target) {
        CloudObject result = CloudObject.forClassName("kind:pair");
        Structs.addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
        return translateStandardCoder(
            result,
            target,
            ImmutableList.of(target.getKeyCoder(), target.getValueCoder()));
      }

      @Override
      public KvCoder<?, ?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponentCoders(cloudObject);
        checkArgument(
            components.size() == 2,
            "Expected 2 component coders for %s, got %s",
            KvCoder.class.getSimpleName(),
            components.size());
        return KvCoder.of(components.get(0), components.get(1));
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
        CloudObject result = CloudObject.forClassName("kind:stream");
        Structs.addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
        return translateStandardCoder(
            result, target, ImmutableList.<Coder<?>>of(target.getElemCoder()));
      }

      @Override
      public IterableCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponentCoders(cloudObject);
        checkArgument(
            components.size() == 1,
            "Expected 1 component coder for %s, got %s",
            IterableCoder.class.getSimpleName(),
            components.size());
        return IterableCoder.of(components.get(0));
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "length_prefix".
   */
  public static CloudObjectTranslator<LengthPrefixCoder<?>> lengthPrefix() {
    return new CloudObjectTranslator<LengthPrefixCoder<?>>() {
      @Override
      public CloudObject toCloudObject(LengthPrefixCoder<?> target) {
        return translateStandardCoder(
            CloudObject.forClassName("kind:length_prefix"),
            target,
            ImmutableList.<Coder<?>>of(target.getValueCoder()));
      }

      @Override
      public LengthPrefixCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponentCoders(cloudObject);
        checkArgument(
            components.size() == 1,
            "Expected 1 component coder for %s, got %s",
            LengthPrefixCoder.class.getSimpleName(),
            components.size());
        return LengthPrefixCoder.of(components.get(0));
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "global_window".
   */
  public static CloudObjectTranslator<GlobalWindow.Coder> globalWindow() {
    return new CloudObjectTranslator<GlobalWindow.Coder>() {
      @Override
      public CloudObject toCloudObject(GlobalWindow.Coder target) {
        return translateStandardCoder(
            CloudObject.forClassName("kind:global_window"),
            target,
            Collections.<Coder<?>>emptyList());
      }

      @Override
      public GlobalWindow.Coder fromCloudObject(CloudObject cloudObject) {
        return GlobalWindow.Coder.INSTANCE;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "interval_window".
   */
  public static CloudObjectTranslator<IntervalWindow.IntervalWindowCoder> intervalWindow() {
    return new CloudObjectTranslator<IntervalWindow.IntervalWindowCoder>() {
      @Override
      public CloudObject toCloudObject(IntervalWindowCoder target) {
        return translateStandardCoder(
            CloudObject.forClassName("kind:interval_window"),
            target,
            Collections.<Coder<?>>emptyList());
      }

      @Override
      public IntervalWindowCoder fromCloudObject(CloudObject cloudObject) {
        return IntervalWindowCoder.of();
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "windowed_value".
   */
  public static CloudObjectTranslator<FullWindowedValueCoder<?>> windowedValue() {
    return new CloudObjectTranslator<FullWindowedValueCoder<?>>() {
      @Override
      public CloudObject toCloudObject(FullWindowedValueCoder<?> target) {
        CloudObject result = CloudObject.forClassName("kind:windowed_value");
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return translateStandardCoder(
            result, target, ImmutableList.of(target.getValueCoder(), target.getWindowCoder()));
      }

      @Override
      public FullWindowedValueCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponentCoders(cloudObject);
        checkArgument(
            components.size() == 2,
            "Expected two Component Coders for %s, got %s",
            FullWindowedValueCoder.class.getSimpleName(),
            components.size());
        return FullWindowedValueCoder.of(
            components.get(0), (Coder<BoundedWindow>) components.get(1));
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is a wrapper
   * for the target class.
   *
   * <p>Used for {@link ValueOnlyWindowedValueCoder}.
   */
  public static CloudObjectTranslator<ValueOnlyWindowedValueCoder<?>> wrapper() {
    return new CloudObjectTranslator<ValueOnlyWindowedValueCoder<?>>() {
      @Override
      public CloudObject toCloudObject(ValueOnlyWindowedValueCoder<?> target) {
        CloudObject result = CloudObject.forClass(target.getClass());
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return translateStandardCoder(
            result, target, ImmutableList.<Coder<?>>of(target.getValueCoder()));
      }

      @Override
      public ValueOnlyWindowedValueCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponentCoders(cloudObject);
        checkArgument(
            components.size() == 1,
            "Expected only one component for %s, got %s",
            ValueOnlyWindowedValueCoder.class.getSimpleName(),
            components.size());
        return ValueOnlyWindowedValueCoder.of(components.get(0));
      }
    };
  }

  public static CloudObjectTranslator<? extends CustomCoder<?>> customCoder() {
    return new CloudObjectTranslator<CustomCoder<?>>() {
      private static final String SERIALIZED_CODER_FIELD = "serialized_coder";
      private static final String UNDERLING_TYPE_FIELD = "type";

      @Override
      public CloudObject toCloudObject(CustomCoder<?> target) {
        return serializeCustomCoder(target);
      }

      private static CloudObject serializeCustomCoder(CustomCoder<?> target) {
        CloudObject result = CloudObject.forClass(CustomCoder.class);
        Structs.addString(result, UNDERLING_TYPE_FIELD, target.getClass().getName());
        Structs.addString(result, SERIALIZED_CODER_FIELD,
            StringUtils.byteArrayToJsonString(
                SerializableUtils.serializeToByteArray(target)));
        return result;
      }

      @Override
      public CustomCoder<?> fromCloudObject(CloudObject cloudObject) {
        String coderClass = (String) cloudObject.get(UNDERLING_TYPE_FIELD);
        String coderBytes = (String) cloudObject.get(SERIALIZED_CODER_FIELD);
        return (CustomCoder<?>) SerializableUtils.deserializeFromByteArray(
            StringUtils.jsonStringToByteArray(coderBytes),
            coderClass);
      }
    };
  }
}
