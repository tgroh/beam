/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.beam.runners.direct.portable;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.RemoteOutputReceiver;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.InProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.PrintingLogWriter;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;

/** A {@link JobBundleFactory} which . */
public class InProcessJobBundleFactory implements JobBundleFactory {

  private final ExecutorService dataExecutor;

  public static JobBundleFactory create() throws IOException {
    ControlClientPool controlClientPool = MapControlClientPool.create();
    ServerFactory serverFactory = InProcessServerFactory.create();
    GrpcFnServer<GrpcLoggingService> logging =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(PrintingLogWriter.getDefault()), serverFactory);
    GrpcFnServer<FnApiControlClientPoolService> control =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                controlClientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);
    EnvironmentFactory environmentFactory =
        InProcessEnvironmentFactory.create(
            PipelineOptionsFactory.create(), logging, control, controlClientPool.getSource());
    return new InProcessJobBundleFactory(environmentFactory, logging, control, serverFactory);
  }

  static JobBundleFactory createForExistingServers(
      EnvironmentFactory factory,
      GrpcFnServer<GrpcLoggingService> logging,
      GrpcFnServer<FnApiControlClientPoolService> control)
      throws IOException {
    return new InProcessJobBundleFactory(
        factory, logging, control, InProcessServerFactory.create());
  }

  private final EnvironmentFactory environmentFactory;

  private final GrpcFnServer<GrpcLoggingService> logging;
  private final GrpcFnServer<FnApiControlClientPoolService> control;

  private final GrpcFnServer<GrpcDataService> data;
  private final GrpcFnServer<GrpcStateService> state;

  private final ConcurrentMap<ExecutableStage, StageBundleFactory<?>> stageBundleFactories =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Environment, RemoteEnvironment> environments =
      new ConcurrentHashMap<>();

  private InProcessJobBundleFactory(
      EnvironmentFactory environmentFactory,
      GrpcFnServer<GrpcLoggingService> logging,
      GrpcFnServer<FnApiControlClientPoolService> control,
      ServerFactory serverFactory)
      throws IOException {
    this.environmentFactory = environmentFactory;
    this.logging = logging;
    this.control = control;
    dataExecutor = Executors.newCachedThreadPool();
    data =
        GrpcFnServer.allocatePortAndCreateFor(GrpcDataService.create(dataExecutor), serverFactory);
    state = GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);
  }

  @Override
  public <T> StageBundleFactory<T> forStage(ExecutableStage executableStage) {
    return (StageBundleFactory<T>)
        stageBundleFactories.computeIfAbsent(executableStage, this::createBundleFactory);
  }

  private final AtomicLong idgen = new AtomicLong();

  private <T> StageBundleFactory<T> createBundleFactory(ExecutableStage stage) {
    RemoteEnvironment remoteEnv =
        environments.computeIfAbsent(
            stage.getEnvironment(),
            env -> {
              try {
                return environmentFactory.createEnvironment(env);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    SdkHarnessClient sdkHarnessClient =
        SdkHarnessClient.usingFnApiClient(
            remoteEnv.getInstructionRequestHandler(), data.getService());
    ExecutableProcessBundleDescriptor descriptor;
    try {
      descriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              Long.toString(idgen.getAndIncrement()), stage, data.getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    RemoteInputDestination<? super WindowedValue<?>> destination =
        descriptor.getRemoteInputDestination();
    SdkHarnessClient.BundleProcessor<T> bundleProcessor =
        sdkHarnessClient.getProcessor(
            descriptor.getProcessBundleDescriptor(),
            (RemoteInputDestination<WindowedValue<T>>) (RemoteInputDestination) destination,
            state.getService());
    return new BundleProcessorStageBundleFactory<>(descriptor, bundleProcessor);
  }

  @Override
  public void close() throws Exception {
    Exception thrown = null;
    for (RemoteEnvironment remoteEnvironment : environments.values()) {
      try {
        remoteEnvironment.close();
      } catch (Exception e) {
        if (thrown == null) {
          thrown = e;
        } else {
          thrown.addSuppressed(e);
        }
      }
    }
    try (AutoCloseable logging = this.logging;
        AutoCloseable control = this.control;
        AutoCloseable dataExec = this.dataExecutor::shutdownNow;
        AutoCloseable data = this.data;
        AutoCloseable state = this.state) {
    } catch (Exception e) {
      if (thrown != null) {
        e.addSuppressed(thrown);
      }
      throw e;
    }
    if (thrown != null) {
      throw thrown;
    }
  }

  private static class BundleProcessorStageBundleFactory<T> implements StageBundleFactory<T> {
    private final ExecutableProcessBundleDescriptor descriptor;
    private final SdkHarnessClient.BundleProcessor<T> processor;

    private BundleProcessorStageBundleFactory(
        ExecutableProcessBundleDescriptor descriptor,
        SdkHarnessClient.BundleProcessor<T> processor) {
      this.descriptor = descriptor;
      this.processor = processor;
    }

    @Override
    public RemoteBundle<T> getBundle(
        OutputReceiverFactory outputReceiverFactory, StateRequestHandler stateRequestHandler) {
      Map<Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Map.Entry<Target, Coder<WindowedValue<?>>> targetCoders :
          descriptor.getOutputTargetCoders().entrySet()) {
        String bundleOutputPCollection =
            Iterables.getOnlyElement(
                descriptor
                    .getProcessBundleDescriptor()
                    .getTransformsOrThrow(targetCoders.getKey().getPrimitiveTransformReference())
                    .getInputsMap()
                    .values());
        FnDataReceiver<WindowedValue<?>> outputReceiver =
            outputReceiverFactory.create(bundleOutputPCollection);
        outputReceivers.put(
            targetCoders.getKey(),
            RemoteOutputReceiver.of(targetCoders.getValue(), outputReceiver));
      }
      return processor.newBundle(outputReceivers, stateRequestHandler);
    }

    @Override
    public void close() {}
  }
}
