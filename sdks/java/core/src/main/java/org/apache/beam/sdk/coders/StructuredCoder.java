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

import static org.apache.beam.sdk.util.Structs.addList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;

/**
 * An abstract base class to implement a {@link Coder} that defines equality, hashing, and printing
 * via the class name and recursively using {@link #getComponents}.
 *
 * <p>To extend {@link StructuredCoder}, override the following methods as appropriate:
 *
 * <ul>
 *   <li>{@link #getComponents}: the default implementation returns {@link #getCoderArguments}.</li>
 *   <li>{@link #getEncodedElementByteSize} and
 *       {@link #isRegisterByteSizeObserverCheap}: the
 *       default implementation encodes values to bytes and counts the bytes, which is considered
 *       expensive.</li>
 * </ul>
 */
public abstract class StructuredCoder<T> extends AbstractCoder<T> {
  protected StructuredCoder() {}

  /**
   * Returns the list of {@link Coder Coders} that are components of this {@link Coder}.
   */
  @Deprecated
  public List<? extends Coder<?>> getComponents() {
    List<? extends Coder<?>> coderArguments = getCoderArguments();
    if (coderArguments == null) {
      return Collections.emptyList();
    } else {
      return coderArguments;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if the two {@link StructuredCoder} instances have the
   * same class and equal components.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    StructuredCoder<?> that = (StructuredCoder<?>) o;
    return this.getComponents().equals(that.getComponents());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() * 31 + getComponents().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    String s = getClass().getName();
    builder.append(s.substring(s.lastIndexOf('.') + 1));

    List<? extends Coder<?>> componentCoders = getComponents();
    if (!componentCoders.isEmpty()) {
      builder.append('(');
      boolean first = true;
      for (Coder<?> componentCoder : componentCoders) {
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        builder.append(componentCoder.toString());
      }
      builder.append(')');
    }
    return builder.toString();
  }

  /**
   * Adds the following properties to the {@link CloudObject} representation:
   * <ul>
   *   <li>component_encodings: A list of coders represented as {@link CloudObject}s
   *       equivalent to the {@link #getCoderArguments}.</li>
   * </ul>
   *
   * <p>{@link StructuredCoder} implementations should override {@link #initializeCloudObject}
   * to customize the {@link CloudObject} representation.
   */
  @Override
  public final CloudObject asCloudObject() {
    CloudObject result = initializeCloudObject();

    List<? extends Coder<?>> components = getComponents();
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder<?> coder : components) {
        cloudComponents.add(coder.asCloudObject());
      }
      addList(result, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    return result;
  }

  /**
   * Subclasses should override this method to customize the {@link CloudObject}
   * representation. {@link StructuredCoder#asCloudObject} delegates to this method
   * to provide an initial {@link CloudObject}.
   *
   * <p>The default implementation returns a {@link CloudObject} using
   * {@link Object#getClass} for the type.
   */
  protected CloudObject initializeCloudObject() {
    return CloudObject.forClass(getClass());
  }

  protected void verifyDeterministic(String message, Iterable<Coder<?>> coders)
      throws NonDeterministicException {
    for (Coder<?> coder : coders) {
      try {
        coder.verifyDeterministic();
      } catch (NonDeterministicException e) {
        throw new NonDeterministicException(this, message, e);
      }
    }
  }

  protected void verifyDeterministic(String message, Coder<?>... coders)
      throws NonDeterministicException {
    verifyDeterministic(message, Arrays.asList(coders));
  }
}
