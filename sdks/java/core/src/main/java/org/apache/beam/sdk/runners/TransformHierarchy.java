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
package org.apache.beam.sdk.runners;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private final TransformTreeNode root;
  private final Map<POutput, TransformTreeNode> producers;
  // Maintain a stack based on the enclosing nodes
  private TransformTreeNode current;

  public TransformHierarchy() {
    root = TransformTreeNode.root(this);
    current = root;
    producers = new HashMap<>();
  }

  public void addNode(String name, PInput input, PTransform<?, ?> transform) {
    // Inputs must be completely specified before they are consumed by a transform.
    input.finishSpecifying();
    TransformTreeNode next =
        TransformTreeNode.subtransform(current, transform, name, input);
    current = next;
  }

  public void setOutput(POutput output) {
    for (PValue value : output.expand()) {
      if (!producers.containsKey(value)) {
        producers.put(value, current);
      }
    }
    output.generateName(current.getTransform(), current.getFullName());
    current.setOutput(output);
  }

  /**
   * Pops the current node off the top of the stack, finishing it.
   */
  public void finishNode() {
    current.finishSpecifying();
    current = current.getEnclosingNode();
    checkState(current != null, "Can't pop the root node of a TransformHierarchy");
  }

  TransformTreeNode getProducer(PValue produced) {
    return producers.get(produced);
  }

  /**
   * Returns all producing transforms for the {@link PValue PValues} contained
   * in {@code output}.
   */
  List<TransformTreeNode> getProducingTransforms(POutput output) {
    List<TransformTreeNode> producingTransforms = new ArrayList<>();
    for (PValue value : output.expand()) {
      TransformTreeNode producer = getProducer(value);
      if (producer != null) {
        producingTransforms.add(producer);
      }
    }
    return producingTransforms;
  }

  public Set<PValue> visit(PipelineVisitor visitor) {
    Set<PValue> visitedValues = new HashSet<>();
    root.visit(visitor, visitedValues);
    return visitedValues;
  }

  public TransformTreeNode getCurrent() {
    return current;
  }
}
