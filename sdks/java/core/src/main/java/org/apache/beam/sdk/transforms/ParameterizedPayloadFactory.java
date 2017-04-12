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

package org.apache.beam.sdk.transforms;

/**
 * A factory for converting a {@link PTransform} to and from a custom serialized form.
 */
public interface ParameterizedPayloadFactory<SourceT extends PTransform, TargetT> {
  /**
   * Gets the specification for.
   *
   * <p>Care should be taken to ensure that the custom part is sufficiently unique to prevent
   * collisions. The returned URN should generally be stable across updates.
   */
  String getCustomUrnPart();

  /**
   * Gets the class that this {@link ParameterizedPayloadFactory} converts into and out of bytes.
   */
  Class<SourceT> getTargetClass();

  /**
   * Convert the provided {@link PTransform} into a target type. This type is suitable for
   * serialization and deserialization, and may be understood by a runner or across languages.
   */
  TargetT toTarget(SourceT obj);

  /**
   * Convert the provided PTransform into a byte array.
   */
  byte[] toBytes(TargetT obj);

  /**
   * Convert the provided byte array into an object.
   */
  TargetT fromBytes(byte[] payload);
}
