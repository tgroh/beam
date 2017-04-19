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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** Created by tgroh on 4/19/17. */
public interface CoderTranslator<T extends StandardCoder<?>, PayloadT extends Serializable> {
  /** Convert the provided payload and components into a {@link Coder}. */
  T fromSerializedForm(PayloadT payload, List<Coder<?>> components) throws IOException;

  /** Convert the provided {@link Coder} into a byte[] payload. */
  PayloadT getPayload(T coder) throws IOException;

  List<Coder<?>> getComponents(T coder);
  // return coder.getComponents();

  interface CoderPayload extends Serializable {
    Coder<?> toCoder(List<Coder<?>> components);
  }
}
