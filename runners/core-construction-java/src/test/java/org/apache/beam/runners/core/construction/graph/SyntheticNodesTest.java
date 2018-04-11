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

package org.apache.beam.runners.core.construction.graph;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SyntheticNodes}. */
@RunWith(JUnit4.class)
public class SyntheticNodesTest {
  @Test
  public void noCollisionsAppendsIndex() {
    String id = SyntheticNodes.uniqueId("foo", o -> false);

    assertThat(id, not(equalTo("foo")));
    assertThat("The original ID should be present in the generated ID", id, containsString("foo"));
  }

  @Test
  public void repeatedProducesUniqueIdsPer() {
    Set<String> synthetics = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      String next = SyntheticNodes.uniqueId("foo", synthetics::contains);
      assertThat(synthetics, not(hasItem(next)));
      synthetics.add(next);
    }
  }
}
