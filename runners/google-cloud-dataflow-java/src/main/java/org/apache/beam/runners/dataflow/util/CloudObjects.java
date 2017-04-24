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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.Structs;

/** Utilities for converting an object to a {@link CloudObject}. */
public class CloudObjects {
  private CloudObjects() {}

  static final Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      CODER_TRANSLATORS = populateCoderInitializers();

  private static Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      populateCoderInitializers() {
    ImmutableMap.Builder<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    // TODO: Implement
    return builder.build();
  }

  public static CloudObject asCloudObject(Coder<?> coder) {
    if (CODER_TRANSLATORS.containsKey(coder.getClass())) {
      // TODO: Make this cast less dangerous
      return standardCoderAsCloudObject((StandardCoder<?>) coder);
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

  /**
   * Convert the provided {@link StandardCoder} to a {@link CloudObject}.
   */
  private static CloudObject standardCoderAsCloudObject(StandardCoder<?> coder) {
    CloudObject result;
    CloudObjectTranslator<StandardCoder<?>> initializer =
        (CloudObjectTranslator<StandardCoder<?>>) CODER_TRANSLATORS.get(coder);
    result = initializer.toCloudObject(coder);

    List<? extends Coder<?>> components = coder.getComponents();
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder<?> component : components) {
        cloudComponents.add(asCloudObject(component));
      }
      Structs.addList(result, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    String encodingId = coder.getEncodingId();
    checkNotNull(encodingId, "Coder.getEncodingId() must not return null.");
    if (!encodingId.isEmpty()) {
      Structs.addString(result, PropertyNames.ENCODING_ID, encodingId);
    }

    Collection<String> allowedEncodings = coder.getAllowedEncodings();
    if (!allowedEncodings.isEmpty()) {
      Structs.addStringList(
          result, PropertyNames.ALLOWED_ENCODINGS, Lists.newArrayList(allowedEncodings));
    }
    return result;
  }

  public static Coder<?> coderFromCloudObject(CloudObject cloudObject) {
    if (cloudObject.getClassName().equals(CustomCoder.class.getName())) {
      return customCoderFromCloudObject(cloudObject);
    }
    throw new IllegalArgumentException(
        String.format("Unknown Cloud Object Class Name %s", cloudObject.getClassName()));
  }

  private static Coder<?> customCoderFromCloudObject(CloudObject cloudObject) {
    String type = Structs.getString(cloudObject, "type");
    String serializedCoder = Structs.getString(cloudObject, "serialized_coder");
    return (CustomCoder<?>)
        SerializableUtils.deserializeFromByteArray(
            StringUtils.jsonStringToByteArray(serializedCoder), type);
  }
}
