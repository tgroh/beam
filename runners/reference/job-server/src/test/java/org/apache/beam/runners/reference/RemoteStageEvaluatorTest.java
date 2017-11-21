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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.local.Bundle;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RemoteStageEvaluator}.
 */
@RunWith(JUnit4.class)
public class RemoteStageEvaluatorTest {
  private org.apache.beam.sdk.Pipeline p =
      TestPipeline.create(
          PipelineOptionsFactory.fromArgs("--runner=org.apache.beam.sdk.testing.CrashingRunner")
              .create());

  private ServerFactory serverFactory = InProcessServerFactory.create();
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private SdkHarnessClientControlService controlService =
      SdkHarnessClientControlService.create();

  private GrpcDataService dataService = GrpcDataService.create(executorService);

  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<SdkHarnessClientControlService> controlServer;
  private GrpcFnServer<GrpcLoggingService> loggingServer;
  private SdkHarnessClient client;

  @Before
  public void setup() throws IOException {
    TupleTag<Integer> outputTag = new TupleTag<Integer>() {};
    p.apply("create", Create.of("foo"))
        .apply("get_len", ParDo.of(new LenFn()).withOutputTags(outputTag, TupleTagList.empty()))
        .get(outputTag)
        .apply("add_one", ParDo.of(new AddOneFn()).withOutputTags(outputTag, TupleTagList.empty()));

    dataServer = GrpcFnServer.allocatePortAndCreateFor(dataService, serverFactory);
    controlServer = GrpcFnServer.allocatePortAndCreateFor(controlService, serverFactory);
    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);

    Pipeline pipelineProto = PipelineTranslation.toProto(p);
    pipelineProto.getComponents().getTransformsOrThrow("get_len");
    pipelineProto.getComponents().getTransformsOrThrow("add_one");
    // TODO: Create ProcessBundleDescriptor with these transforms, plus remote reads and writes
    RemoteGrpcPortRead read =
        RemoteGrpcPortRead.readFromPort(
            RemoteGrpcPort.newBuilder()
                .setApiServiceDescriptor(dataServer.getApiServiceDescriptor())
                .build(),
            "local_read_output");
    RemoteGrpcPortWrite write =
        RemoteGrpcPortWrite.writeToPort(
            "local_write_input",
            RemoteGrpcPort.newBuilder()
                .setApiServiceDescriptor(dataServer.getApiServiceDescriptor())
                .build());

    executorService.submit(
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            FnHarness.main(
                PipelineOptionsFactory.create(),
                controlServer.getApiServiceDescriptor(),
                loggingServer.getApiServiceDescriptor());
            return null;
          }
        });

    // Blocks until the FnHarness connects
    client = controlService.getClient();
  }

  @Test
  public void testRemoteExecution() throws Exception {
    PipelineStage stage;
    Bundle<String> elems =
        new Bundle<String>() {
          @Override
          public Instant getMinimumTimestamp() {
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
          }

          @Override
          public Iterator<WindowedValue<String>> iterator() {
            return ImmutableList.of(
                    WindowedValue.valueInGlobalWindow("foo"),
                    WindowedValue.valueInGlobalWindow("quux"))
                .iterator();
          }
        };
    RemoteStageEvaluator<String> evaluator = RemoteStageEvaluator.create(stage, client, elems);
  }

  private static class LenFn extends DoFn<String, Integer> {
    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output(ctxt.element().length());
    }
  }

  private static class AddOneFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output(ctxt.element() + 1);
    }
  }
}
