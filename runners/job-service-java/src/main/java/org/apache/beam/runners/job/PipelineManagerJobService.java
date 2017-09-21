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

package org.apache.beam.runners.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.JobNotFoundException;
import org.apache.beam.runners.core.construction.PipelineManager;
import org.apache.beam.runners.core.construction.PipelineManager.PipelineMessage;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.common.runner.v1.JobApi;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobMessage.MessageImportance;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobMessagesRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobMessagesResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobState.JobStateType;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceImplBase;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A service implementing the Beam Job API on top of a {@link PipelineManager}. */
public class PipelineManagerJobService extends JobServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineManagerJobService.class);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private final PipelineManager runner;

  public PipelineManagerJobService(PipelineManager runner) {
    this.runner = runner;
  }

  @Override
  public void prepare(
      JobApi.PrepareJobRequest request,
      StreamObserver<JobApi.PrepareJobResponse> responseObserver) {}

  @Override
  public void run(
      JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {}

  @Override
  public void getState(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} {}", GetJobStateRequest.class.getSimpleName(), request);
    try {
      State currentState = runner.getState(request.getJobId());
      responseObserver.onNext(
          GetJobStateResponse.newBuilder().setState(toProtoState(currentState)).build());
      responseObserver.onCompleted();
    } catch (JobNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    LOG.trace("{} {}", CancelJobRequest.class.getSimpleName(), request);
    try {
      State cancelledState = runner.cancel(request.getJobId());
      responseObserver.onNext(
          CancelJobResponse.newBuilder().setState(toProtoState(cancelledState)).build());
      responseObserver.onCompleted();
    } catch (JobNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @Override
  public void getStateStream(
      GetJobStateRequest request, StreamObserver<GetJobStateResponse> responseObserver) {
    LOG.trace("{} as stream {}", GetJobStateRequest.class.getSimpleName(), request);
    try {
      responseObserver.onNext(
          GetJobStateResponse.newBuilder()
              .setState(toProtoState(runner.getState(request.getJobId())))
              .build());
      Iterator<PipelineManager.StateOrMessage> updateStream =
          runner.getUpdateStream(request.getJobId());
      while (updateStream.hasNext()) {
        PipelineManager.StateOrMessage update = updateStream.next();
        if (update.getState() != null) {
          responseObserver.onNext(
              GetJobStateResponse.newBuilder().setState(toProtoState(update.getState())).build());
        }
      }
      // There will be no more updates to the update stream
      responseObserver.onCompleted();
    } catch (JobNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @Override
  public void getMessageStream(
      JobMessagesRequest request, StreamObserver<JobMessagesResponse> responseObserver) {
    LOG.trace("{} as stream {}", JobMessagesRequest.class.getSimpleName(), request);
    try {
      Iterator<PipelineManager.StateOrMessage> updates =
          runner.getUpdateStream(request.getJobId());
      while (updates.hasNext()) {
        PipelineManager.StateOrMessage update = updates.next();
        if (update.getMessage() != null) {
          responseObserver.onNext(
              JobMessagesResponse.newBuilder()
                  .setMessageResponse(toProtoMessage(update.getMessage()))
                  .build());
        }
        if (update.getState() != null) {
          responseObserver.onNext(
              JobMessagesResponse.newBuilder()
                  .setStateResponse(
                      GetJobStateResponse.newBuilder().setState(toProtoState(update.getState())))
                  .build());
        }
      }
      responseObserver.onCompleted();
    } catch (JobNotFoundException e) {
      responseObserver.onError(Status.NOT_FOUND.asException());
    }
  }

  @AutoValue
  abstract static class StateOrMessage {
    static StateOrMessage message(PipelineMessage message) {
      return new AutoValue_PipelineManagerJobService_StateOrMessage(message, null);
    }

    static StateOrMessage state(PipelineResult.State state) {
      return new AutoValue_PipelineManagerJobService_StateOrMessage(null, state);
    }

    @Nullable
    abstract PipelineMessage getMessage();
    @Nullable
    abstract State getState();
  }

  private JobApi.JobMessage toProtoMessage(PipelineManager.PipelineMessage message) {
    return JobApi.JobMessage.newBuilder()
        .setMessageId(message.getId())
        .setMessageText(message.getText())
        .setTime(ISODateTimeFormat.basicDateTime().print(message.getTimestamp()))
        .setImportance(toProto(message.getImportance()))
        .build();
  }

  private MessageImportance toProto(PipelineManager.PipelineMessage.Importance importance) {
    switch (importance) {
      case BASIC:
        return MessageImportance.JOB_MESSAGE_BASIC;
      case DEBUG:
        return MessageImportance.JOB_MESSAGE_DEBUG;
      case DETAILED:
        return MessageImportance.JOB_MESSAGE_DETAILED;
      case ERROR:
        return MessageImportance.JOB_MESSAGE_ERROR;
      case WARNING:
        return MessageImportance.JOB_MESSAGE_WARNING;
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s %s", MessageImportance.class.getSimpleName(), importance));
    }
  }

  private JobApi.JobState.JobStateType toProtoState(State currentState) {
    switch (currentState) {
      case UNKNOWN:
        return JobStateType.UNKNOWN;
      case RUNNING:
        return JobStateType.RUNNING;
      case UPDATED:
        return JobStateType.UPDATED;
      case STOPPED:
        return JobStateType.STOPPED;
      case CANCELLED:
        return JobStateType.CANCELLED;
      case FAILED:
        return JobStateType.FAILED;
      case DONE:
        return JobStateType.DONE;
      default:
        throw new IllegalStateException(String.format("Unknown job state %s", currentState));
    }
  }
}
