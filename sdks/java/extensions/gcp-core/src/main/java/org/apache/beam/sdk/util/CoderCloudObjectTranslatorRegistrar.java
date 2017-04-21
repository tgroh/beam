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

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;

/**
 * A way to register {@link CloudObjectTranslator CloudObjectInitializers} for a type of
 * {@link Coder}.
 */
public interface CoderCloudObjectRegistrar {
  /**
   * Gets a mapping from Java Class to a {@link CloudObjectTranslator} specialized for that class.
   */
  Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> getJavaClasses();

  /**
   * Gets a mapping from a {@link CloudObject} class (as returned by {@link
   * CloudObject#getClassName()}) to a {@link CloudObjectTranslator} that can translate from that
   * class.
   */
  Map<String, CloudObjectTranslator<? extends Coder>> getCloudObjectClasses();
}
