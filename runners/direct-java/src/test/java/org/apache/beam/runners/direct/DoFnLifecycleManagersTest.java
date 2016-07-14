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

import static org.hamcrest.Matchers.containsInAnyOrder;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.common.collect.ImmutableList;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link DoFnLifecycleManagers}.
 */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagersTest {
  @Rule ExpectedException thrown = ExpectedException.none();

  @Test
  public void removeAllWhenManagersThrowSuppressesAndThrows() throws Exception {
    DoFnLifecycleManager first = DoFnLifecycleManager.of(new ThrowsInCleanupFn("foo"));
    DoFnLifecycleManager second = DoFnLifecycleManager.of(new ThrowsInCleanupFn("bar"));
    DoFnLifecycleManager third = DoFnLifecycleManager.of(new ThrowsInCleanupFn("baz"));
    first.get();
    second.get();
    third.get();

    final List<Matcher<Exception>> suppressions = new ArrayList<>();
    suppressions.add()

    thrown.expect(
        new BaseMatcher<Exception>() {
          @Override
          public void describeTo(Description description) {
            description
                .appendText("Exception suppressing ")
                .appendList("[", ", ", "]", suppressions);
          }

          @Override
          public boolean matches(Object item) {
            if (!(item instanceof Exception)) {
              return false;
            }
            Exception that = (Exception) item;
            return containsInAnyOrder(suppressions).matches(that.getSuppressed());
          }
        });

    DoFnLifecycleManagers.removeAllFromManagers(ImmutableList.of(first, second, third));
  }

  @Test
  public void whenManagersSucceedSucceeds() {

  }

  private class ThrowsInCleanupFn extends DoFn<Object, Object> {
  }
}
