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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.SimpleProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.RemoteOutputReceiver;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that executes an {@link ExecutableStage} on a remote SDK
 * Harness via the Beam Portability Framework.
 */
class RemoteStageEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext ctxt;

  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<GrpcDataService> dataServer;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private final BlockingQueue<FnApiControlClient> clientPool;
  private final ExecutorService dataExecutor;

  private final EnvironmentManager environmentManager;

  RemoteStageEvaluatorFactory(EvaluationContext ctxt) throws IOException {
    this.ctxt = ctxt;
    ServerFactory serverFactory = ServerFactory.createDefault();
    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    dataExecutor = Executors.newCachedThreadPool();
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcDataService.create(dataExecutor), serverFactory);
    clientPool = new SynchronousQueue<>();
    controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(clientPool), serverFactory);
    environmentManager = null;
  }

  private final Supplier<String> nextId =
      new Supplier<String>() {
        private final AtomicLong next = new AtomicLong(-1L);

        @Override
        public String get() {
          return Long.toString(next.getAndDecrement());
        }
      };
  private final ConcurrentMap<ExecutableStage, SimpleProcessBundleDescriptor> stageDescriptors =
      new ConcurrentHashMap<>();

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    Components components = Components.getDefaultInstance();
    ExecutableStage stage = null;
    SimpleProcessBundleDescriptor descriptor =
        stageDescriptors.computeIfAbsent(
            stage,
            executableStage ->
                ProcessBundleDescriptors.fromExecutableStage(
                    nextId.get(), stage, components, dataServer.getApiServiceDescriptor()));
    RemoteEnvironment environment = environmentManager.getEnvironment(stage.getEnvironment());
    SdkHarnessClient client = environment.getClient();
    return new RemoteStageEvaluator<>(application, ctxt, client, descriptor);
  }

  @Override
  public void cleanup() throws Exception {
    dataServer.close();
    dataExecutor.shutdown();
    controlServer.close();
    loggingServer.close();

    dataExecutor.shutdownNow();
  }

  private static class RemoteStageEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final AppliedPTransform<?, ?, ?> transform;

    private final ActiveBundle<InputT> bundle;
    private final Map<PCollection<?>, UncommittedBundle<?>> pcollectionBundles = new HashMap<>();

    private RemoteStageEvaluator(
        AppliedPTransform<?, ?, ?> transform,
        EvaluationContext ctxt,
        SdkHarnessClient client,
        SimpleProcessBundleDescriptor desc) {
      this.transform = transform;
      // TODO: Side inputs, when we support side inputs in the ReferenceRunner
      Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Map.Entry<BeamFnApi.Target, Coder<WindowedValue<?>>> outputCoder :
          desc.getOutputTargetCoders().entrySet()) {
        // TODO: Get from the context, or the graph
        PCollection<?> output = PCollection.createPrimitiveOutputInternal(null, null, null, null);
        UncommittedBundle<?> bundle =
            pcollectionBundles.computeIfAbsent(output, ctxt::createBundle);
        RemoteOutputReceiver<?> receiver =
            RemoteOutputReceiver.of(
                outputCoder.getValue(), elem -> bundle.add((WindowedValue) elem));
        outputReceivers.put(outputCoder.getKey(), receiver);
      }
      this.bundle =
          client
              .getProcessor(
                  desc.getProcessBundleDescriptor(),
                  (RemoteInputDestination<WindowedValue<InputT>>)
                      (RemoteInputDestination) desc.getRemoteInputDestination())
              .newBundle(outputReceivers);
    }

    @Override
    public void processElement(WindowedValue<InputT> element) throws Exception {
      bundle.getInputReceiver().accept(element);
    }

    @Override
    public TransformResult<InputT> finishBundle() throws Exception {
      // TODO: State updates, when we support user state in portability
      // TODO: Timer updates, when we support user timers in portability
      for (InboundDataClient inboundDataClient : bundle.getOutputClients().values()) {
        inboundDataClient.awaitCompletion();
      }
      return StepTransformResult.<InputT>withoutHold(transform)
          .addOutput(pcollectionBundles.values())
          .build();
    }
  }
}
