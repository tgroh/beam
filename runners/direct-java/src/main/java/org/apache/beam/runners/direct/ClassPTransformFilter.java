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

import org.apache.beam.sdk.runners.PTransformFilter;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link PTransformFilter} that matches all {@link PTransform PTransforms} matching a specified
 * class. For all other {@link PTransform PTransforms}, returns {@link Match#ENTER}.
 */
class ClassPTransformFilter implements PTransformFilter {
  public static ClassPTransformFilter of(Class<? extends PTransform> clazz) {
    return new ClassPTransformFilter(clazz);
  }

  private final Class<? extends PTransform> clazz;

  private ClassPTransformFilter(Class<? extends PTransform> clazz) {
    this.clazz = clazz;
  }

  @Override
  public Match match(TransformTreeNode node) {
    if (node.getTransform() != null && clazz.equals(node.getTransform().getClass())) {
      return Match.REPLACE;
    }
    return Match.ENTER;
  }
}
