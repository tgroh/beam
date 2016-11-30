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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TransformHierarchy}.
 */
@RunWith(JUnit4.class)
public class TransformHierarchyTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private TransformHierarchy hierarchy;
  private TestPipeline pipeline;

  @Before
  public void setup() {
    hierarchy = new TransformHierarchy();
    pipeline = TestPipeline.create();
  }

  @Test
  public void getCurrentNoPushReturnsRoot() {
    assertThat(hierarchy.getCurrent().isRootNode(), is(true));
  }

  @Test
  public void popWithoutPushThrows() {
    thrown.expect(IllegalStateException.class);
    hierarchy.finishNode();
  }

  @Test
  public void pushThenPopSucceeds() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    PBegin begin = PBegin.in(pipeline);
    Create.Values<Integer> create = Create.of(1);
    TransformHierarchy.Node newNode = hierarchy.addNode("Create", begin, create);
    assertThat(hierarchy.getCurrent(), equalTo(newNode));
    assertThat(newNode.getEnclosingNode(), equalTo(root));
    assertThat(newNode.getInput(), Matchers.<PInput>equalTo(begin));
    assertThat(newNode.getTransform(), Matchers.<PTransform<?, ?>>equalTo(create));
    hierarchy.finishNode();
    assertThat(hierarchy.getCurrent(), equalTo(root));
  }

  @Test
  public void visitVisitsAllPushed() {
    TransformHierarchy.Node root = hierarchy.getCurrent();
    PBegin begin = PBegin.in(pipeline);
    Create.Values<Integer> create = Create.of(1);
    Read.Bounded<Long> read = Read.from(CountingSource.upTo(1L));
    PCollection<Long> created =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    MapElements<Long, Long> map =
        MapElements.via(
            new SimpleFunction<Long, Long>() {
              @Override
              public Long apply(Long input) {
                return input;
              }
            });
    PCollection<Long> mapped =
        PCollection.createPrimitiveOutputInternal(
            pipeline, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

    // TODO: Tests that the intermediate primitive was finished specifying
    TransformHierarchy.Node compositeNode = hierarchy.addNode("Create", begin, create);

    TransformHierarchy.Node primitiveNode = hierarchy.addNode("Create/Read", begin, read);
    hierarchy.setOutput(created);
    hierarchy.finishNode();

    hierarchy.setOutput(created);
    hierarchy.finishNode();

    TransformHierarchy.Node otherPrimitive = hierarchy.addNode("ParDo", created, map);
    assertThat(created.isFinishedSpecifyingInternal(), is(true));
    hierarchy.setOutput(mapped);
    hierarchy.finishNode();


    final Set<TransformHierarchy.Node> visitedCompositeNodes = new HashSet<>();
    final Set<TransformHierarchy.Node> visitedPrimitiveNodes = new HashSet<>();
    final Set<PValue> visitedValuesInVisitor = new HashSet<>();

    Set<PValue> visitedValues =
        hierarchy.visit(
            new PipelineVisitor.Defaults() {
              @Override
              public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
                visitedCompositeNodes.add(node);
                return CompositeBehavior.ENTER_TRANSFORM;
              }

              @Override
              public void visitPrimitiveTransform(TransformHierarchy.Node node) {
                visitedPrimitiveNodes.add(node);
              }

              @Override
              public void visitValue(PValue value, TransformHierarchy.Node producer) {
                visitedValuesInVisitor.add(value);
              }
            });

    assertThat(visitedCompositeNodes, containsInAnyOrder(root, compositeNode));
    assertThat(visitedPrimitiveNodes, containsInAnyOrder(primitiveNode, otherPrimitive));
    assertThat(visitedValuesInVisitor, Matchers.<PValue>containsInAnyOrder(created, mapped));
    assertThat(visitedValues, equalTo(visitedValuesInVisitor));
  }
}
