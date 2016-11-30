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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private final Node root;
  private final Map<POutput, Node> producers;
  // Maintain a stack based on the enclosing nodes
  private Node current;

  public TransformHierarchy() {
    root = new Node(null, null, "", null);
    current = root;
    producers = new HashMap<>();
  }

  /**
   * Adds the named {@link PTransform} consuming the provided {@link PInput} as a node in this
   * {@link TransformHierarchy} as a child of the current node, and sets it to be the current node.
   *
   * <p>This call should be finished by expanding and recursively calling {@link #addNode(String,
   * PInput, PTransform)}, setting the output with {@link #setOutput(POutput)}, followed by a call
   * to {@link #finishNode()}.
   *
   * @return the added node
   */
  public Node addNode(String name, PInput input, PTransform<?, ?> transform) {
    // Inputs must be completely specified before they are consumed by a transform.
    input.finishSpecifying();
    current = subTransform(current, transform, name, input);
    return current;
  }

  /**
   * Sets the output of the current {@link Node} to be the provided node. For each
   * {@link PValue} contained in the result of {@link POutput#expand()}, adds the current node as
   * the producer of that node if it does not have an existing producer, and validates the output.
   */
  public void setOutput(POutput output) {
    for (PValue value : output.expand()) {
      if (!producers.containsKey(value)) {
        producers.put(value, current);
      }
    }
    // TODO: Replace with a "generateDefaultNames" method.
    output.recordAsOutput(current.toAppliedPTransform());
    current.setOutput(output);
  }

  /**
   * Pops the current node off the top of the stack, finishing it.
   */
  public void finishNode() {
    current.finishSpecifying();
    current = current.getEnclosingNode();
    checkState(current != null, "Can't finish the root node of a TransformHierarchy");
  }

  public Node getProducer(PValue produced) {
    return producers.get(produced);
  }

  /**
   * Returns all producing transforms for the {@link PValue PValues} contained
   * in {@code output}.
   */
  public List<Node> getProducingTransforms(POutput output) {
    List<Node> producingTransforms = new ArrayList<>();
    for (PValue value : output.expand()) {
      Node producer = getProducer(value);
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

  public Node getCurrent() {
    return current;
  }

  private Node subTransform(
      Node enclosing, PTransform<?, ?> transform, String fullName, PInput input) {
    checkNotNull(enclosing);
    checkNotNull(transform);
    checkNotNull(fullName);
    checkNotNull(input);
    Node node = new Node(enclosing, transform, fullName, input);
    current.addComposite(node);
    return node;
  }

  public class Node {
    private final Node enclosingNode;

    // The PTransform for this node, which may be a composite PTransform.
    // The root of a TransformHierarchy is represented as a Node
    // with a null transform field.
    private final PTransform<?, ?> transform;

    private final String fullName;

    // Nodes for sub-transforms of a composite transform.
    private final Collection<Node> parts = new ArrayList<>();

    // Input to the transform, in unexpanded form.
    private final PInput input;

    // TODO: track which outputs need to be exported to parent.
    // Output of the transform, in unexpanded form.
    private POutput output;

    private boolean finishedSpecifying = false;

    /**
     * Creates a new Node with the given parent and transform.
     *
     * <p>EnclosingNode and transform may both be null for a root-level node, which holds all other
     * nodes.
     *
     * @param enclosingNode the composite node containing this node
     * @param transform the PTransform tracked by this node
     * @param fullName the fully qualified name of the transform
     * @param input the unexpanded input to the transform
     */
    private Node(
        @Nullable Node enclosingNode,
        @Nullable PTransform<?, ?> transform,
        String fullName,
        @Nullable PInput input) {
      this.enclosingNode = enclosingNode;
      this.transform = transform;
      this.fullName = fullName;
      this.input = input;
    }

    /**
     * Returns the transform associated with this transform node.
     */
    public PTransform<?, ?> getTransform() {
      return transform;
    }

    /**
     * Returns the enclosing composite transform node, or null if there is none.
     */
    public Node getEnclosingNode() {
      return enclosingNode;
    }

    /**
     * Adds a composite operation to the transform node.
     *
     * <p>As soon as a node is added, the transform node is considered a
     * composite operation instead of a primitive transform.
     */
    public void addComposite(Node node) {
      parts.add(node);
    }

    /**
     * Returns true if this node represents a composite transform that does not perform
     * processing of its own, but merely encapsulates a sub-pipeline (which may be empty).
     *
     * <p>Note that a node may be composite with no sub-transforms if it  returns its input directly
     * extracts a component of a tuple, or other operations that occur at pipeline assembly time.
     */
    public boolean isCompositeNode() {
      return !parts.isEmpty() || returnsOthersOutput() || isRootNode();
    }

    private boolean returnsOthersOutput() {
      PTransform<?, ?> transform = getTransform();
      if (output != null) {
        for (PValue outputValue : output.expand()) {
          if (!outputValue.getProducingTransformInternal().getTransform().equals(transform)) {
            return true;
          }
        }
      }
      return false;
    }

    public boolean isRootNode() {
      return transform == null;
    }

    public String getFullName() {
      return fullName;
    }

    /**
     * Returns the transform input, in unexpanded form.
     */
    public PInput getInput() {
      return input;
    }

    /**
     * Adds an output to the transform node, validating that it is applicable.
     *
     * <p>A composite node must not produce any outputs. All outputs of a composite node should be
     * produced by some other primitive node. A primitive node must output only {@link PValue PValues}
     * it produced (usually with {@link PCollection#createPrimitiveOutputInternal(Pipeline,
     * WindowingStrategy, IsBounded)}).
     *
     * <p>{@link #setOutput(POutput)} should be called only once, before the node is finished with
     * {@link #finishSpecifying()}. Outputs will not be finished until they are consumed.
     */
    public void setOutput(POutput output) {
      checkState(!finishedSpecifying);
      checkState(this.output == null, "Tried to specify more than one output for %s", getFullName());

      // Validate that a primitive transform produces only primitive output, and a composite transform
      // does not produce primitive output.
      List<Node> producingTransforms = getProducingTransforms(output);
      if (isCompositeNode()) {
        for (Node producingTransform : producingTransforms) {
          // Using == because object identity indicates that the transforms
          // are the same node in the pipeline
          if (this == producingTransform ) {
            throw new IllegalStateException(
                "Output of composite transform "
                    + transform
                    + " is registered as being produced by it,"
                    + " but the output of every composite transform should be"
                    + " produced by a primitive transform contained therein.");
          }
        }
      } else {
        for (Node producingTransform : producingTransforms) {
          // Using != because object identity indicates that the transforms
          // are the same node in the pipeline
          if (this != producingTransform) {
            throw new IllegalArgumentException(
                "Output of non-composite transform "
                    + transform
                    + " is registered as being produced by"
                    + " a different transform: "
                    + producingTransform);
          }
        }
      }

      this.output = output;
    }

    /**
     * Returns the transform output, in unexpanded form.
     */
    public POutput getOutput() {
      return output;
    }

    /**
     * Visit the transform node.
     *
     * <p>Provides an ordered visit of the input values, the primitive
     * transform (or child nodes for composite transforms), then the
     * output values.
     */
    public void visit(PipelineVisitor visitor,
        Set<PValue> visitedValues) {
      if (!finishedSpecifying) {
        finishSpecifying();
      }

      if (!isRootNode()) {
        // Visit inputs.
        for (PValue inputValue : input.expand()) {
          if (visitedValues.add(inputValue)) {
            visitor.visitValue(inputValue, getProducer(inputValue));
          }
        }
      }

      if (isCompositeNode()) {
        PipelineVisitor.CompositeBehavior recurse = visitor.enterCompositeTransform(this);

        if (recurse.equals(CompositeBehavior.ENTER_TRANSFORM)) {
          for (Node child : parts) {
            child.visit(visitor, visitedValues);
          }
        }
        visitor.leaveCompositeTransform(this);
      } else {
        visitor.visitPrimitiveTransform(this);
      }

      if (!isRootNode()) {
        // Visit outputs.
        for (PValue pValue : output.expand()) {
          if (visitedValues.add(pValue)) {
            visitor.visitValue(pValue, this);
          }
        }
      }
    }

    /**
     * Finish specifying a transform.
     *
     * <p>All inputs are finished first, then the transform, then
     * all outputs.
     */
    public void finishSpecifying() {
      if (finishedSpecifying) {
        return;
      }
      finishedSpecifying = true;
    }

    /**
     * Convert this {@link Node} to an {@link AppliedPTransform}.
     *
     * @deprecated used for temporary compatibility with recordAsOutput.
     */
    @Deprecated
    AppliedPTransform<?,?,?> toAppliedPTransform() {
      @SuppressWarnings("rawtypes")
      AppliedPTransform<?, ?, ?> appliedTransform =
          AppliedPTransform.of(fullName, input, output, (PTransform) transform);
      return appliedTransform;
    }

  }
}
