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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StandardCoder;

/**
 * Utilities for converting an object to a {@link CloudObject}.
 */
public class CloudObjects {
  private CloudObjects() {}

  private static final Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      coderTranslators = populateCoderInitializers();
  private static final Map<String, CloudObjectTranslator<? extends Coder>> cloudObjectKeys =
      populateCloudObjectKeys();

  private static Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      populateCoderInitializers() {
    ImmutableMap.Builder<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    for (CoderCloudObjectTranslatorRegistrar registrar :
        ServiceLoader.load(CoderCloudObjectTranslatorRegistrar.class)) {
      builder.putAll(registrar.getJavaClasses());
    }
    return builder.build();
  }

  private static Map<String, CloudObjectTranslator<? extends Coder>> populateCloudObjectKeys() {
    ImmutableMap.Builder<String, CloudObjectTranslator<? extends Coder>> builder =
        ImmutableMap.builder();
    for (CoderCloudObjectTranslatorRegistrar registrar :
        ServiceLoader.load(CoderCloudObjectTranslatorRegistrar.class)) {
      builder.putAll(registrar.getCloudObjectClasses());
    }
    return builder.build();
  }

  public static CloudObject asCloudObject(Coder<?> coder) {
    if (coder instanceof StandardCoder && coderTranslators.containsKey(coder.getClass())) {
      // TODO: Make this cast less dangerous
      return asCloudObject((StandardCoder<?>) coder);
    } else if (coder instanceof CustomCoder) {
      return asCloudObject((CustomCoder<?>) coder);
    }
    throw new IllegalArgumentException(
        String.format(
            "Non-Custom %s with no registered %s", Coder.class, CloudObjectTranslator.class));
  }

  public static Coder<?> fromCloudObject(CloudObject coder) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  private static CloudObject asCloudObject(CustomCoder<?> coder) {
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
  private static CloudObject asCloudObject(StandardCoder<?> coder) {
    CloudObject result = initializeCloudObject(coder);
    List<? extends Coder<?>> components = coder.getComponents();
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder<?> component : components) {
        cloudComponents.add(component.asCloudObject());
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

  private static <CoderT extends Coder<?>> CloudObject initializeCloudObject(CoderT coder) {
    CloudObjectTranslator<CoderT> initializer =
        (CloudObjectTranslator<CoderT>) coderTranslators.get(coder);
    if (initializer != null) {
      return initializer.toCloudObject(coder);
    } else {
      return CloudObject.forClass(coder.getClass());
    }
  }
}
