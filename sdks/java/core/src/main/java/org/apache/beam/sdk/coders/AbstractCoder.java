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

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.ByteArrayOutputStream;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An abstract base class for {@link Coder} that provides some default method implementations.
 * Generally, users should extend {@link CustomCoder} instead of this class.
 *
 * <p>{@link AbstractCoder} provides:
 *
 * <ul>
 *   <li>{@link #getEncodedElementByteSize} and {@link #isRegisterByteSizeObserverCheap}: the
 *       default implementation encodes values to bytes and counts the bytes, which is considered
 *       expensive.
 *   <li>{@link #consistentWithEquals()}, returning false
 *   <li>{@link #structuralValue(Object)}, returning the value if the coder is consistent with
 *       equals and an object based on the encoded representation otherwise
 * </ul>
 */
public abstract class AbstractCoder<T> implements Coder<T> {
  /**
   * {@inheritDoc}
   *
   * @return {@code false} unless it is overridden. {@link StructuredCoder#registerByteSizeObserver}
   *         invokes {@link #getEncodedElementByteSize} which requires re-encoding an element
   *         unless it is overridden. This is considered expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(T value, Context context) {
    return false;
  }

  /**
   * Returns the size in bytes of the encoded value using this coder.
   */
  protected long getEncodedElementByteSize(T value, Context context)
      throws Exception {
    try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
      encode(value, os, context);
      return os.getCount();
    } catch (Exception exn) {
      throw new IllegalArgumentException(
          "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>For {@link StructuredCoder} subclasses, this notifies {@code observer} about the byte size
   * of the encoded value using this coder as returned by {@link #getEncodedElementByteSize}.
   */
  @Override
  public void registerByteSizeObserver(
      T value, ElementByteSizeObserver observer, Context context)
      throws Exception {
    observer.update(getEncodedElementByteSize(value, context));
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code false} for {@link StructuredCoder} unless overridden.
   */
  @Override
  public boolean consistentWithEquals() {
    return false;
  }

  @Override
  public Object structuralValue(T value) {
    if (value != null && consistentWithEquals()) {
      return value;
    } else {
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(value, os, Context.OUTER);
        return new StructuralByteArray(os.toByteArray());
      } catch (Exception exn) {
        throw new IllegalArgumentException(
            "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return (TypeDescriptor<T>)
        TypeDescriptor.of(getClass()).resolveType(new TypeDescriptor<T>() {}.getType());
  }
}
