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
package org.apache.beam.sdk.values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PCollectionLists.
 */
@RunWith(JUnit4.class)
public class PCollectionListTest {
  @Test
  public void testEmptyListFailure() {
    try {
      PCollectionList.of(Collections.<PCollection<String>>emptyList());
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      assertThat(
          exn.toString(),
          containsString(
              "must either have a non-empty list of PCollections, "
              + "or must first call empty(Pipeline)"));
    }
  }

  @Test
  public void testIterationOrder() {
    Pipeline p = TestPipeline.create();
    PCollection<Long> createOne = p.apply("CreateOne", Create.of(1L, 2L, 3L));
    PCollection<Long> boundedCount = p.apply("CountBounded", CountingInput.upTo(23L));
    PCollection<Long> unboundedCount = p.apply("CountUnbounded", CountingInput.unbounded());
    PCollection<Long> createTwo = p.apply("CreateTwo", Create.of(-1L, -2L));
    PCollection<Long> maxRecordsCount =
        p.apply("CountLimited", CountingInput.unbounded().withMaxNumRecords(22L));

    ImmutableList<PCollection<Long>> counts =
        ImmutableList.of(boundedCount, maxRecordsCount, unboundedCount);
    // Contains is the order-dependent matcher
    PCollectionList<Long> pcList = PCollectionList.of(counts);
    assertThat(
        pcList.getAll(),
        contains(boundedCount, maxRecordsCount, unboundedCount));

    PCollectionList<Long> withOneCreate = pcList.and(createTwo);
    assertThat(
        withOneCreate.getAll(), contains(boundedCount, maxRecordsCount, unboundedCount, createTwo));

    PCollectionList<Long> fromEmpty =
        PCollectionList.<Long>empty(p)
            .and(unboundedCount)
            .and(createOne)
            .and(ImmutableList.of(boundedCount, maxRecordsCount));
    assertThat(
        fromEmpty.getAll(), contains(unboundedCount, createOne, boundedCount, maxRecordsCount));

    List<TaggedPValue> expansion = fromEmpty.expand();
    // TaggedPValues are stable between expansions
    assertThat(expansion, equalTo(fromEmpty.expand()));

    List<PCollection<Long>> expectedList =
        ImmutableList.of(unboundedCount, createOne, boundedCount, maxRecordsCount);
    for (int i = 0; i < expansion.size(); i++) {
      assertThat(
          "Index " + i + " should have equal PValue",
          expansion.get(i).getValue(),
          Matchers.<PValue>equalTo(expectedList.get(i)));
    }
  }

  @Test
  public void testExpansionOrderWithDuplicates() {
    TestPipeline p = TestPipeline.create();
    PCollection<Long> firstCount = p.apply("CountFirst", CountingInput.upTo(10L));
    PCollection<Long> secondCount = p.apply("CountSecond", CountingInput.upTo(10L));

    PCollectionList<Long> counts =
        PCollectionList.of(firstCount).and(secondCount).and(firstCount).and(firstCount);

    ImmutableList<PCollection<Long>> expectedOrder =
        ImmutableList.of(firstCount, secondCount, firstCount, firstCount);
    assertThat(counts.expand(), hasSize(4));
    for (int i = 0; i < 4; i++) {
      assertThat(
          "Index " + i + " should be equal",
          counts.expand().get(i).getValue(),
          Matchers.<PValue>equalTo(expectedOrder.get(i)));
    }
  }
}
