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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.util.Structs.addBoolean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An {@link IterableCoder} encodes any {@link Iterable} in the format of {@link
 * IterableLikeCoderBase}.
 *
 * @param <T> the type of the elements of the iterables being transcoded
 */
public class IterableCoder<T> extends StandardCoder<Iterable<T>>
    implements IterableLikeCoder<T, Iterable<T>> {

  public static <T> IterableCoder<T> of(Coder<T> elemCoder) {
    return new IterableCoder<>(elemCoder);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.
  private final Helper<T> helper;
  private final Coder<T> elemCoder;

  @JsonCreator
  public static IterableCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
    return of(components.get(0));
  }

  /**
   * Returns the first element in this iterable if it is non-empty,
   * otherwise returns {@code null}.
   */
  public static <T> List<Object> getInstanceComponents(
      Iterable<T> exampleValue) {
    return Helper.getInstanceComponentsHelper(exampleValue);
  }

  protected IterableCoder(Coder<T> elemCoder) {
    this.helper = new Helper<>(elemCoder);
    this.elemCoder = elemCoder;
  }

  @Override
  protected CloudObject initializeCloudObject() {
    CloudObject result = CloudObject.forClassName("kind:stream");
    addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
    return result;
  }

  @Override
  public void encode(
      Iterable<T> value, OutputStream outStream, Context context)
      throws CoderException, IOException {

  }

  @Override
  public Iterable<T> decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return helper.decode(inStream, context);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(getElemCoder());
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    helper.verifyDeterministic();
  }

  @Override
  public boolean consistentWithEquals() {
    return helper.consistentWithEquals();
  }

  @Override
  public Object structuralValue(Iterable<T> value) {
    return helper.structuralValue(value);
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(
      Iterable<T> value, Context context) {
    return helper.isRegisterByteSizeObserverCheap(value, context);
  }

  @Override
  public void registerByteSizeObserver(
      Iterable<T> value, ElementByteSizeObserver observer, Context context) throws Exception {
    helper.registerByteSizeObserver(value, observer, context);
  }

  @Override
  public String getEncodingId() {
    return helper.getEncodingId();
  }

  @Override
  public Collection<String> getAllowedEncodings() {
    return helper.getAllowedEncodings();
  }

  @Override
  public TypeDescriptor<Iterable<T>> getEncodedTypeDescriptor() {
    return helper.getEncodedTypeDescriptor();
//    return new TypeDescriptor<Iterable<T>>() {}.where(
//        new TypeParameter<T>() {}, getElemCoder().getEncodedTypeDescriptor());
  }

  @Override
  public Coder<T> getElemCoder() {
    return elemCoder;
  }

  private static final class Helper<T> extends IterableLikeCoderBase<T, Iterable<T>> {
    private Helper(Coder<T> elemCoder) {
      super(elemCoder, "Iterable");
    }

    @Override
    protected Iterable<T> decodeToIterable(List<T> decodedElements) {
      return decodedElements;
    }
  }
}
