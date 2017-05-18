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

package org.apache.beam.runners.core.construction;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow.Coder;

/**
 * Created by tgroh on 5/18/17.
 */
public class RunnerApiUrns {
  /** An URN representing the Java SDK environment. */
  // TODO: make this environemnt representative of the current version of the SDK
  @Experimental(Kind.CORE_RUNNERS_ONLY)
  public static final String JAVA_SDK_URN = "urn:beam:environment:javasdk:0.1";

  // This URN says that the coder is just a UDF blob this SDK understands
  /**
   * A UDF blob that is understood by the Java SDK as a java serialized {@link Coder}.
   *
   * TODO: standardize such things
   */
  public static final String JAVA_SERIALIZED_CODER_URN = "urn:beam:coders:javasdk:0.1";

  /** A UDF blob that is understood by the Java SDK as a java serialized {@link GlobalCombineFn}. */
  public static final String JAVA_SERIALIZED_COMBINE_FN_URN = "urn:beam:combine:javasdk:0.1";

  /** A UDF blob that is understood by the Java SDK as a java serialized {@link BoundedSource}. */
  public static final String JAVA_SERIALIZED_BOUNDED_SOURCE = "urn:beam:source:javasdk:bounded:0.1";

  /** A UDF blob that is understood by the Java SDK as a java serialized {@link UnboundedSource}. */
  public static final String JAVA_SERIALIZED_UNBOUNDED_SOURCE =
      "urn:beam:source:javasdk:unbounded:0.1";
}
