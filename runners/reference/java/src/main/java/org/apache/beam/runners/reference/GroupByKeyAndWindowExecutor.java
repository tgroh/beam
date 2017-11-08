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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MergeStatus.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * Created by tgroh on 11/9/17.
 */
public class GroupByKeyAndWindowExecutor<K, InT, OutT> {
  private final RunnerApi.WindowingStrategy windowingStrategy;

  public GroupByKeyAndWindowExecutor(RunnerApi.WindowingStrategy windowingStrategy) {
    checkArgument(
        windowingStrategy.getMergeStatus().equals(RunnerApi.MergeStatus.Enum.NON_MERGING),
        "Only a %s %s is supported",
        Enum.NON_MERGING,
        WindowingStrategy.class.getSimpleName());
    this.windowingStrategy = windowingStrategy;
  }

  public void processElement(KV<ByteString, WindowedValue<ByteString>> element) throws Exception {
    ReduceFnRunner<ByteString, ByteString, Iterable<ByteString>, BoundedWindow> reduceFnRunner =
        new ReduceFnRunner<>(
            element.getKey(),
            windowingStrategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(windowingStrategy.getTrigger())),
            stateInternals,
            timerInternals,
            new OutputWindowedValueToBundle<>(bundle) /* TODO: Buffering stuff */,
            new UnsupportedSideInputReader(GroupByKeyAndWindowExecutor.class.getSimpleName()),
            SystemReduceFn.buffering(ByteStringCoder.of()),
            null /* PipelineOptions are required only for CombineWithContext */);
    reduceFnRunner.processElements(
        Collections.<WindowedValue<ByteString>>singleton(element.getValue()));
  }

  public void processTimer(TimerData timer) {

  }

  private static class ByteStringCoder extends Coder<ByteString> {
    public static Coder<ByteString> of() {
      return new ByteStringCoder();
    }

    @Override
    public void encode(ByteString value, OutputStream outStream)
        throws CoderException, IOException {
      int size = value.size();
      VarInt.encode(size, outStream);
      value.writeTo(outStream);
    }

    @Override
    public ByteString decode(InputStream inStream) throws CoderException, IOException {
      int size = VarInt.decodeInt(inStream);
      return ByteString.readFrom(inStream, size);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
