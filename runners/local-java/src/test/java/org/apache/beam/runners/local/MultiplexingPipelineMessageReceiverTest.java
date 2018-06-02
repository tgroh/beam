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

package org.apache.beam.runners.local;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MultiplexingPipelineMessageReceiver}. */
@RunWith(JUnit4.class)
public class MultiplexingPipelineMessageReceiverTest {
  @Test
  public void emptySucceeds() {
    MultiplexingPipelineMessageReceiver rcvr = MultiplexingPipelineMessageReceiver.create();
    rcvr.failed(new AssertionError());
    rcvr.failed(new RuntimeException());
    rcvr.completed();
    rcvr.cancelled();
  }

  @Test
  public void addSucceeds() {
    MultiplexingPipelineMessageReceiver rcvr = MultiplexingPipelineMessageReceiver.create();
    PipelineMessageReceiver target = mock(PipelineMessageReceiver.class);
    AssertionError error = new AssertionError("foo");
    rcvr.failed(error);
    verify(target).failed(error);
    RuntimeException exception = new RuntimeException("bar");
    rcvr.failed(exception);
    verify(target).failed(exception);
    rcvr.completed();
    verify(target).completed();
    rcvr.cancelled();
    verify(target).cancelled();
  }

  @Test
  public void addMultipleSucceeds() {
    MultiplexingPipelineMessageReceiver rcvr = MultiplexingPipelineMessageReceiver.create();
    PipelineMessageReceiver first = mock(PipelineMessageReceiver.class);
    PipelineMessageReceiver second = mock(PipelineMessageReceiver.class);
    rcvr.addReceiver(first);
    rcvr.addReceiver(second);
    AssertionError error = new AssertionError("foo");
    rcvr.failed(error);
    verify(first).failed(error);
    verify(second).failed(error);
    RuntimeException exception = new RuntimeException("bar");
    rcvr.failed(exception);
    verify(first).failed(exception);
    verify(second).failed(exception);
    rcvr.completed();
    verify(first).completed();
    verify(second).completed();
    rcvr.cancelled();
    verify(first).cancelled();
    verify(second).cancelled();
  }
}
