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

package org.apache.beam.sdk.fn.data;

/**
 * TODO: Document
 */
public interface ManagedDataStream {
  /**
   * Cancels the stream, causing it to drop any future inbound data.
   */
  void cancel();

  /**
   * Mark the stream as completed.
   */
  void complete();

  /**
   * Mark the client as completed with an exception. Calls to awaitCompletion will terminate by
   * throwing the provided exception.
   *
   * @param t the throwable that caused this client to fail
   */
  void fail(Throwable t);
}
