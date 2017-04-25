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

package org.apache.beam.sdk.coders;

/**
 * A {@link Coder} for a class that implements {@code Iterable}.
 *
 * <p>The format of this coder is as follows:
 *
 * <ul>
 *   <li>If the input {@link Iterable} has a known and finite size, then the size is written to the
 *       output stream in big endian format, followed by all of the encoded elements.</li>
 *   <li>If the input {@link Iterable} is not known to have a finite size, then each element
 *       of the input is preceded by {@code true} encoded as a byte (indicating "more data")
 *       followed by the encoded element, and terminated by {@code false} encoded as a byte.</li>
 * </ul>
 *
 * @param <T> the type of the elements of the {@code Iterable}s being transcoded
 * @param <IterableT> the type of the Iterables being transcoded
 */
public interface IterableLikeCoder<T, IterableT extends Iterable<T>> extends Coder<IterableT> {
  /**
   * Gets the coder for the elements of the returned Iterable.
   */
  Coder<T> getElemCoder();
}
