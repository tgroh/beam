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

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link PipelineOptions} to configure the {@link JobApiRunner}.
 */
public interface BeamJobApiOptions extends PipelineOptions {
  String getJobServiceHost();
  void setJobServiceHost(String jobServiceHost);

  Integer getJobServicePort();
  void setJobServicePort(Integer jobServicePort);

  String getJobServiceTarget();
  void setJobServiceTarget(String jobServiceTarget);

  @Hidden
  @Internal
  @Default.InstanceFactory(ChannelInstanceFactory.class)
  Channel getJobApiChannel();
  void setJobApiChannel();

  /**
   * A {@link DefaultValueFactory} which creates a {@link Channel} for use by the Job API from the
   * options in {@link BeamJobApiOptions}.
   */
  class ChannelInstanceFactory implements DefaultValueFactory<Channel> {
    @Override
    public Channel create(PipelineOptions options) {
      ManagedChannelBuilder<?> channelBuilder;
      BeamJobApiOptions jobApiOptions = options.as(BeamJobApiOptions.class);
      if (jobApiOptions.getJobServiceTarget() == null) {
        checkArgument(
            jobApiOptions.getJobServiceHost() != null,
            "JobServiceHost must be provided if JobServiceTarget is not set");
        checkArgument(
            jobApiOptions.getJobServicePort() != null,
            "JobServicePort must be provided if JobServiceTarget is not set");
        channelBuilder =
            ManagedChannelBuilder.forAddress(
                jobApiOptions.getJobServiceHost(), jobApiOptions.getJobServicePort());
      } else {
        channelBuilder = ManagedChannelBuilder.forTarget(jobApiOptions.getJobServiceTarget());
      }
      channelBuilder.usePlaintext(true);
      return channelBuilder.build();
    }
  }
}
