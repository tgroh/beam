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

package org.apache.beam.runners.core.construction;

import com.google.auto.value.AutoValue;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/** An enriched interface to interact with a {@link PipelineRunner} over the lifecycle of a job. */
public interface PipelineManager {
  String run(String jobName, Pipeline pipeline, PipelineOptions options);

  /**
   * Returns the current state of the Job with the specified ID.
   */
  PipelineResult.State getState(String jobId) throws JobNotFoundException;

  /**
   * Cancels the job with the specified ID. Returns the current state after the job has been
   * cancelled.
   */
  PipelineResult.State cancel(String jobId) throws JobNotFoundException;

  /**
   * Returns a stream of message and state updates for the provided JobId.
   *
   * <p>The returned iterator should have the following behavior:
   *
   * <ul>
   *   <li>{@link Iterator#hasNext()} should return false only when there cannot be more messages
   *       produced by the pipeline. Generally, this can be when the job is in a terminal state.
   *   <li>{@link Iterator#next()} should never return null.
   * </ul>
   */
  Iterator<StateOrMessage> getUpdateStream(String jobId) throws JobNotFoundException;

  /**
   * A {@link PipelineMessage} containing updates from the {@link PipelineRunner}.
   */
  @AutoValue
  abstract class PipelineMessage {
    public static PipelineMessage of(
        String id, Instant timestamp, Importance importance, String text) {
      return new AutoValue_PipelineManager_PipelineMessage(id, timestamp, importance, text);
    }

    /** Returns the ID of this message. */
    public abstract String getId();

    /**
     * Returns the timestamp of this message.
     */
    public abstract Instant getTimestamp();

    /**
     * Return the importance of this message.
     */
    public abstract Importance getImportance();

    /**
     * Returns the text of this message.
     */
    public abstract String getText();

    /**
     * The importance of a {@link PipelineMessage}.
     */
    public enum Importance {
      DEBUG,
      DETAILED,
      BASIC,
      WARNING,
      ERROR
    }
  }

  /** A union type of {@link PipelineResult.State} and {@link PipelineManager.PipelineMessage}. */
  @AutoValue
  abstract class StateOrMessage {
    public static StateOrMessage state(PipelineResult.State state) {
      return new AutoValue_PipelineManager_StateOrMessage(null, state);
    }

    public static StateOrMessage message(PipelineManager.PipelineMessage message) {
      return new AutoValue_PipelineManager_StateOrMessage(message, null);
    }

    @Nullable
    public abstract PipelineManager.PipelineMessage getMessage();

    @Nullable
    public abstract PipelineResult.State getState();
  }
}
