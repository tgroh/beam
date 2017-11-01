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

package org.apache.beam.runners.reference.job;

import com.google.auto.value.AutoValue;
import java.io.File;
import org.apache.beam.artifact.local.LocalFileSystemArtifactStagerService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;

/** A Beam Job that is not yet ready to execute. */
@AutoValue
public abstract class PendingJob {
  /**
   * Create a new {@link PendingJob.Builder}.
   */
  public static PendingJob.Builder builder() {
    return new AutoValue_PendingJob.Builder();
  }

  /** Get the root directory in which artifacts will be staged. */
  public abstract File getArtifactStagingLocation();

  /**
   * Get the server used to stage artifacts for this Job.
   */
  public abstract GrpcFnServer<LocalFileSystemArtifactStagerService> getStagerServer();

  /**
   * A Builder for {@link PendingJob Pending Jobs}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Set the root directory in which artifacts will be staged. */
    public abstract Builder setArtifactStagingLocation(File stagingLocation);

    /**
     * Set the server used to stage artifacts for this Job.
     */
    public abstract Builder setStagerServer(
        GrpcFnServer<LocalFileSystemArtifactStagerService> service);

    public abstract PendingJob build();
  }
}
