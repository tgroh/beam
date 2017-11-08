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

package org.apache.beam.runners.reference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;

/**
 * Created by tgroh on 11/7/17.
 */
public class ContainerManager {
  private final Multimap<Environment, Harness> environmentHarnesses = HashMultimap.create();

  private ContainerManager(Iterable<RunnerApi.Environment> environments) {
    for (RunnerApi.Environment environment : environments) {
      environment.getUrl();
    }
  }

  private static class Harness implements AutoCloseable {
    private final RunnerApi.Environment environment;

    private Harness(Environment environment) {
      this.environment = environment;
    }

    @Override
    public void close() throws Exception {
      // TODO: implement actual container shutdown
    }
  }
}
