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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.EncodabilityEnforcementFactoryTest.Record;
import org.apache.beam.runners.direct.EncodabilityEnforcementFactoryTest.RecordNoDecodeCoder;
import org.apache.beam.runners.direct.EncodabilityEnforcementFactoryTest.RecordNoEncodeCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CloningBundleFactory}.
 */
@RunWith(JUnit4.class)
public class CloningBundleFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private CloningBundleFactory factory = CloningBundleFactory.create();

  @Test
  public void rootBundleWorkingCoderSucceedsClonesOutput() {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.of(1, 3).withCoder(VarIntCoder.of()));
    WindowedValue<Integer> one = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    CommittedBundle<Integer> root =
        factory.createRootBundle(created).add(one).add(three).commit(Instant.now());

    assertThat(root.getElements(), containsInAnyOrder(one, three));
    assertThat(root.getElements(), not(containsInAnyOrder(theInstance(one), theInstance(three))));
    assertThat(root.getPCollection(), equalTo(created));
    assertThat(root.getKey(), Matchers.<StructuralKey<?>>equalTo(StructuralKey.unkeyed()));
  }

  @Test
  public void bundleWorkingCoderSucceedsClonesOutput() {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.of(1, 3).withCoder(VarIntCoder.of()));
    CommittedBundle<Integer> root =
        factory
            .createKeyedBundle(null, StructuralKey.of(99L, VarLongCoder.of()), created)
            .commit(Instant.now());

    PCollection<KV<String, Integer>> kvs =
        created
            .apply(WithKeys.<String, Integer>of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    WindowedValue<KV<String, Integer>> fooOne = WindowedValue.valueInGlobalWindow(KV.of("foo", 1));
    WindowedValue<KV<String, Integer>> fooThree =
        WindowedValue.valueInGlobalWindow(KV.of("foo", 3));
    CommittedBundle<KV<String, Integer>> bundle =
        factory.createBundle(root, kvs).add(fooOne).add(fooThree).commit(Instant.now());

    assertThat(bundle.getElements(), containsInAnyOrder(fooOne, fooThree));
    assertThat(
        bundle.getElements(), not(containsInAnyOrder(theInstance(fooOne), theInstance(fooThree))));
    for (WindowedValue<KV<String, Integer>> foo : bundle.getElements()) {
      assertThat(
          foo.getValue(),
          not(anyOf(theInstance(fooOne.getValue()), theInstance(fooThree.getValue()))));
    }
    assertThat(bundle.getPCollection(), equalTo(kvs));
    assertThat(bundle.getKey(), Matchers.<StructuralKey<?>>equalTo(root.getKey()));
  }

  @Test
  public void keyedBundleWorkingCoderSucceedsClonesOutput() {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.of(1, 3).withCoder(VarIntCoder.of()));
    CommittedBundle<Integer> root = factory.createRootBundle(created).commit(Instant.now());
    PCollection<KV<String, Integer>> kvs =
        created
            .apply(WithKeys.<String, Integer>of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    CommittedBundle<KV<String, Integer>> intermediate =
        factory.createBundle(root, kvs).commit(Instant.now());

    PCollection<KV<String, Iterable<Integer>>> keyed =
        kvs.apply(GroupByKey.<String, Integer>create());
    WindowedValue<KV<String, Iterable<Integer>>> foos =
        WindowedValue.valueInGlobalWindow(
            KV.<String, Iterable<Integer>>of("foo", ImmutableList.of(1, 3)));
    CommittedBundle<KV<String, Iterable<Integer>>> keyedBundle =
        factory
            .createKeyedBundle(intermediate, StructuralKey.of("foo", StringUtf8Coder.of()), keyed)
            .add(foos)
            .commit(Instant.now());

    assertThat(keyedBundle.getElements(), containsInAnyOrder(foos));
    assertThat(
        Iterables.getOnlyElement(keyedBundle.getElements()).getValue(),
        not(theInstance(foos.getValue())));
    assertThat(keyedBundle.getPCollection(), equalTo(keyed));
    assertThat(
        keyedBundle.getKey(),
        Matchers.<StructuralKey<?>>equalTo(StructuralKey.of("foo", StringUtf8Coder.of())));
  }

  @Test
  public void rootBundleEncodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoEncodeCoder()));
    UncommittedBundle<Record> bundle = factory.createRootBundle(pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void rootBundleDecodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoDecodeCoder()));
    UncommittedBundle<Record> bundle = factory.createRootBundle(pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void bundleEncodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoEncodeCoder()));
    CommittedBundle<Record> root = factory.createRootBundle(pc).commit(Instant.now());
    UncommittedBundle<Record> bundle = factory.createBundle(root, pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void bundleDecodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoDecodeCoder()));
    CommittedBundle<Record> root = factory.createRootBundle(pc).commit(Instant.now());
    UncommittedBundle<Record> bundle = factory.createBundle(root, pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void keyedBundleEncodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoEncodeCoder()));
    CommittedBundle<Record> root = factory.createRootBundle(pc).commit(Instant.now());
    UncommittedBundle<Record> bundle =
        factory.createKeyedBundle(root, StructuralKey.of("foo", StringUtf8Coder.of()), pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void keyedBundleDecodeFailsAddFails() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> pc = p.apply(Create.<Record>of().withCoder(new RecordNoDecodeCoder()));
    CommittedBundle<Record> root = factory.createRootBundle(pc).commit(Instant.now());
    UncommittedBundle<Record> bundle =
        factory.createKeyedBundle(root, StructuralKey.of("foo", StringUtf8Coder.of()), pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }
}
