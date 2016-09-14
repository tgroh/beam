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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformFilter.MatchResponse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private final Deque<TransformTreeNode> transformStack = new LinkedList<>();
  private final Map<PInput, TransformTreeNode> producingTransformNode = new HashMap<>();

  /**
   * Create a {@code TransformHierarchy} containing a root node.
   */
  public TransformHierarchy() {
    // First element in the stack is the root node, holding all child nodes.
    transformStack.add(new TransformTreeNode(null, null, "", null));
  }

  /**
   * Returns the last TransformTreeNode on the stack.
   */
  public TransformTreeNode getCurrent() {
    return transformStack.peek();
  }

  /**
   * Add a TransformTreeNode to the stack.
   */
  public void pushNode(TransformTreeNode current) {
    transformStack.push(current);
  }

  /**
   * Removes the most recent TransformTreeNode from the stack.
   */
  public void popNode() {
    transformStack.pop();
    checkState(!transformStack.isEmpty());
  }

  /**
   * Adds an input to the given node.
   *
   * <p>This forces the producing node to be finished.
   */
  public void addInput(TransformTreeNode node, PInput input) {
    for (PValue i : input.expand()) {
      TransformTreeNode producer = producingTransformNode.get(i);
      checkState(producer != null, "Producer unknown for input: %s", i);

      producer.finishSpecifying();
      node.addInputProducer(i, producer);
    }
  }

  /**
   * Sets the output of a transform node.
   */
  public void setOutput(TransformTreeNode producer, POutput output) {
    producer.setOutput(output);

    for (PValue o : output.expand()) {
      producingTransformNode.put(o, producer);
    }
  }

  /**
   * Visits all nodes in the transform hierarchy, in transitive order.
   */
  public void visit(Pipeline.PipelineVisitor visitor,
                    Set<PValue> visitedNodes) {
    transformStack.peekFirst().visit(visitor, visitedNodes);
  }

  public void reset() {
  }

  public void replace(Map<TransformFilter, TransformOverrideFactory> factories) {
    FilterAndReplaceVisitor visitor = new FilterAndReplaceVisitor(factories);
    visit(visitor, new HashSet<PValue>());
    Map<TransformTreeNode, PTransform<?, ?>> overrides = visitor.nodeToOverrides;
    for (Map.Entry<TransformTreeNode, PTransform<?, ?>> nodeOverride : overrides.entrySet()) {
      replace(nodeOverride.getKey(), nodeOverride.getValue());
    }
  }

  private void replace(TransformTreeNode original, PTransform<?, ?> replacementTransform) {
    TransformTreeNode enclosing = original.getEnclosingNode();
    transformStack.push(enclosing);
    String nodeName = original.getName();
    enclosing.startReplace(original);
    // This output is discarded -
    POutput output =
        Pipeline.applyTransform(nodeName, original.getInput(), (PTransform) replacementTransform);
    enclosing.finishedReplace(original);
    transformStack.pop();
  }

  private class FilterAndReplaceVisitor extends PipelineVisitor.Defaults {
    private final Map<TransformFilter, TransformOverrideFactory> factories;
    private final Map<TransformTreeNode, PTransform<?, ?>> nodeToOverrides;

    private FilterAndReplaceVisitor(
        Map<TransformFilter, TransformOverrideFactory> factories) {
      this.factories = factories;
      nodeToOverrides = new HashMap<>();
    }

    @Override
    public CompositeBehavior enterCompositeTransform(final TransformTreeNode node) {
      return matchAndReplace(node);
    }

    private CompositeBehavior matchAndReplace(TransformTreeNode node) {
      CompositeBehavior matched = CompositeBehavior.ENTER_TRANSFORM;
      TransformOverrideFactory factory = null;
      for (Map.Entry<TransformFilter, TransformOverrideFactory> filterFactory : factories.entrySet()) {
        MatchResponse match = filterFactory.getKey().matches(node);
        switch (match) {
          case REPLACE:
            factory = filterFactory.getValue();
            checkArgument(matched == CompositeBehavior.ENTER_TRANSFORM);
            matched = CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            break;
          case SKIP_OVER:
            checkArgument(matched == CompositeBehavior.ENTER_TRANSFORM);
            matched = CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            break;
          case CONTINUE:
            break;
        }
      }
      if (factory != null) {
        nodeToOverrides.put(node, factory.getOverride(node));
      }
      return matched;
    }

    @Override
    public void visitPrimitiveTransform(TransformTreeNode node) {
      // Discard the returned composite behavior, but replace the node if necessary
      matchAndReplace(node);
    }
  }
}
