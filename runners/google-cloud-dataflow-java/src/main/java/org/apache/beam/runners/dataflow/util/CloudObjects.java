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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.Structs;
import org.apache.beam.sdk.util.WindowedValue;

/** Utilities for converting an object to a {@link CloudObject}. */
public class CloudObjects {
  private CloudObjects() {}

  static final Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      CODER_TRANSLATORS = defaultCoderTranslators();

  private static Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      defaultCoderTranslators() {
    return ImmutableMap.<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>builder()
        .put(GlobalWindow.Coder.class, CloudObjectTranslators.globalWindow())
        .put(IntervalWindow.IntervalWindowCoder.class, CloudObjectTranslators.intervalWindow())
        .put(LengthPrefixCoder.class, CloudObjectTranslators.lengthPrefix())
        .put(IterableCoder.class, CloudObjectTranslators.stream())
        .put(KvCoder.class, CloudObjectTranslators.pair())
        .put(WindowedValue.FullWindowedValueCoder.class, CloudObjectTranslators.windowedValue())
        .put(WindowedValue.ValueOnlyWindowedValueCoder.class, CloudObjectTranslators.wrapper())
        .build();
  }

  public static CloudObject asCloudObject(Coder<?> coder) {
    CloudObjectTranslator<Coder<?>> translator =
        (CloudObjectTranslator<Coder<?>>) CODER_TRANSLATORS.get(coder.getClass());
    if (translator != null) {
      return translator.toCloudObject(coder);
    } else if (coder instanceof CustomCoder) {
      return customCoderAsCloudObject((CustomCoder<?>) coder);
    }
    throw new IllegalArgumentException(
        String.format(
            "Non-Custom %s with no registered %s", Coder.class, CloudObjectTranslator.class));
  }

  private static CloudObject customCoderAsCloudObject(CustomCoder<?> coder) {
    CloudObject result = CloudObject.forClass(CustomCoder.class);
    Structs.addString(result, "type", coder.getClass().getName());
    Structs.addString(
        result,
        "serialized_coder",
        StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(coder)));

    return result;
  }

  public static Coder<?> coderFromCloudObject(CloudObject cloudObject) {
    List<Coder<?>> componentCoders = getComponentCodersFromCloudObject(cloudObject);
    if (cloudObject.getClassName().equals(CustomCoder.class.getName())) {
      return customCoderFromCloudObject(cloudObject);
    } else if (cloudObject
        .getClassName()
        .equals(WindowedValue.ValueOnlyWindowedValueCoder.class.getName())) {
      // This if statement can't be in the switch, because the class name is resolved at runtime.
      checkComponentSize(componentCoders, 1, LengthPrefixCoder.class);
      return WindowedValue.getValueOnlyCoder(componentCoders.get(0));
    }
    switch (cloudObject.getClassName()) {
      case CloudObjectKinds.KIND_GLOBAL_WINDOW:
        checkComponentSize(componentCoders, 0, GlobalWindow.Coder.class);
        return GlobalWindow.Coder.INSTANCE;
      case CloudObjectKinds.KIND_INTERVAL_WINDOW:
        checkComponentSize(componentCoders, 0, IntervalWindow.IntervalWindowCoder.class);
        return IntervalWindow.IntervalWindowCoder.of();
      case CloudObjectKinds.KIND_LENGTH_PREFIX:
        checkComponentSize(componentCoders, 1, LengthPrefixCoder.class);
        return LengthPrefixCoder.of(componentCoders.get(0));
      case CloudObjectKinds.KIND_STREAM:
        checkComponentSize(componentCoders, 1, IterableCoder.class);
        return IterableCoder.of(componentCoders.get(0));
      case CloudObjectKinds.KIND_PAIR:
        checkComponentSize(componentCoders, 2, KvCoder.class);
        return KvCoder.of(componentCoders.get(0), componentCoders.get(1));
      case CloudObjectKinds.KIND_WINDOWED_VALUE:
        checkComponentSize(componentCoders, 2, WindowedValue.FullWindowedValueCoder.class);
        return WindowedValue.getFullCoder(
            componentCoders.get(0), (Coder<BoundedWindow>) componentCoders.get(1));
      default:
        throw new IllegalArgumentException(
            String.format("Unknown Cloud Object Class Name %s", cloudObject.getClassName()));
    }
  }

  private static void checkComponentSize(
      List<Coder<?>> componentCoders, int i, Class<?> lengthPrefixCoderClass) {
    checkArgument(
        componentCoders.size() == i,
        "Unexpected number of Components for coder of type %s. Expected: %s Got: %s",
        lengthPrefixCoderClass.getSimpleName(),
        i,
        componentCoders.size());
  }

  public static void addComponentCodersToCloudObject(CloudObject target, List<Coder<?>> components) {
    List<CloudObject> componentCoders = new ArrayList<>();
    for (Coder<?> coder : components) {
      componentCoders.add(asCloudObject(coder));
    }
    Structs.addList(target, PropertyNames.COMPONENT_ENCODINGS, componentCoders);
  }

  private static List<Coder<?>> getComponentCodersFromCloudObject(CloudObject cloudObject) {
    List<Map<String, Object>> components =
        Structs.getListOfMaps(
            cloudObject,
            PropertyNames.COMPONENT_ENCODINGS,
            Collections.<Map<String, Object>>emptyList());
    List<Coder<?>> componentCoders = new ArrayList<>();
    for (Map<String, Object> component : components) {
      Coder<?> coder = coderFromCloudObject(CloudObject.fromSpec(component));
      componentCoders.add(coder);
    }
    return componentCoders;
  }

  private static Coder<?> customCoderFromCloudObject(CloudObject cloudObject) {
    String type = Structs.getString(cloudObject, "type");
    String serializedCoder = Structs.getString(cloudObject, "serialized_coder");
    return (CustomCoder<?>)
        SerializableUtils.deserializeFromByteArray(
            StringUtils.jsonStringToByteArray(serializedCoder), type);
  }
}
