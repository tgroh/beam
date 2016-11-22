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

import static com.google.common.base.Preconditions.checkState;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * A pipeline visitor that tracks all keyed {@link PValue PValues}. A {@link PValue} is keyed if it
 * is the result of a {@link PTransform} that produces keyed outputs. A {@link PTransform} that
 * produces keyed outputs is assumed to colocate output elements that share a key.
 *
 * <p>All {@link GroupByKey} transforms, or their runner-specific implementation primitive, produce
 * keyed output.
 */
// TODO: Handle Key-preserving transforms when appropriate and more aggressively make PTransforms
// unkeyed
class StateAccessingTrackingVisitor extends PipelineVisitor.Defaults {
  @SuppressWarnings("rawtypes")
  private final Set<Class<? extends PTransform>> accessesState;
  private final Set<AppliedPTransform<?, ?, ?>> matched;
  private boolean finalized;

  public static StateAccessingTrackingVisitor create(
      @SuppressWarnings("rawtypes") Set<Class<? extends PTransform>> producesKeyedOutputs) {
    return new StateAccessingTrackingVisitor(producesKeyedOutputs);
  }

  private StateAccessingTrackingVisitor(
      @SuppressWarnings("rawtypes") Set<Class<? extends PTransform>> producesKeyedOutputs) {
    this.accessesState = producesKeyedOutputs;
    this.matched = new HashSet<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    checkState(
        !finalized,
        "Attempted to use a %s that has already been finalized on a pipeline (visiting node %s)",
        StateAccessingTrackingVisitor.class.getSimpleName(),
        node);
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
    checkState(
        !finalized,
        "Attempted to use a %s that has already been finalized on a pipeline (visiting node %s)",
        StateAccessingTrackingVisitor.class.getSimpleName(),
        node);
    if (node.isRootNode()) {
      finalized = true;
    } else {
      checkState(!accessesState.contains(node.getTransform().getClass()),
          "Composite Transform Types should not be listed as accessing state: %s in node %s",
          node.getTransform().getClass().getSimpleName(),
          node.getFullName());
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
    if (accessesState.contains(node.getTransform().getClass())) {
      matched.add(
          AppliedPTransform.<PInput, POutput, PTransform>of(
              node.getFullName(), node.getInput(), node.getOutput(), node.getTransform()));
    }
  }

  public Set<AppliedPTransform<?, ?, ?>> getStateAccessingTransforms() {
    checkState(
        finalized,
        "can't call getStateAccessingTransforms before a Pipeline has been completely traversed");
    return matched;
  }
}
