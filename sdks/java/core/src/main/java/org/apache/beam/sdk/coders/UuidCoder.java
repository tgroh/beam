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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Created by tgroh on 5/17/17.
 */
public class UuidCoder extends AtomicCoder<UUID> {
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();
  private static final UuidCoder INSTANCE = new UuidCoder();

  public static UuidCoder of() {
    return INSTANCE;
  }

  private UuidCoder() {}

  @Override
  public void encode(UUID value, OutputStream outStream) throws CoderException, IOException {
    LONG_CODER.encode(value.getMostSignificantBits(), outStream);
    LONG_CODER.encode(value.getLeastSignificantBits(), outStream);
  }

  @Override
  public UUID decode(InputStream inStream) throws CoderException, IOException {
    long mostSigBits = LONG_CODER.decode(inStream);
    long leastSigBits = LONG_CODER.decode(inStream);
    return new UUID(mostSigBits, leastSigBits);
  }

  @Override
  public void verifyDeterministic() {
    LONG_CODER.verifyDeterministic();
  }

  /**
   * {@inheritDoc}.
   *
   * @return true. {@link UuidCoder} is injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public long getEncodedElementByteSize(UUID element) throws Exception {
    return LONG_CODER.getEncodedElementByteSize(element.getMostSignificantBits())
        + LONG_CODER.getEncodedElementByteSize(element.getLeastSignificantBits());
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(UUID value) {
    return LONG_CODER.isRegisterByteSizeObserverCheap(value.getMostSignificantBits())
        && LONG_CODER.isRegisterByteSizeObserverCheap(value.getLeastSignificantBits());
  }
}
