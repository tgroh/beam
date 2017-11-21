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

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container to manage {@link FnService} servers.
 */
public class FnServerManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FnServerManager.class);

  /**
   * Create a new {@link FnServerManager} that will use the {@link ServerFactory} to create new
   * servers.
   */
  public static FnServerManager create(ServerFactory serverFactory) {
    return new FnServerManager(serverFactory);
  }

  private final ServerFactory serverFactory;
  // TODO: Make this more real
  private final Set<AutoCloseable> activeServers;

  private FnServerManager(ServerFactory serverFactory) {
    this.serverFactory = serverFactory;
    // this.loggingService = null;
    // this.controlService = null;
    activeServers = new HashSet<>();
  }

  @Override
  public void close() throws Exception {
    for (AutoCloseable active : activeServers) {
      try {
        active.close();
      } catch (Exception e) {
        LOG.error("Exception while closing server {}", active, e);
      }
    }
  }
}
