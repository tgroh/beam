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

package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;

/**
 * A {@link WindowFn} that assigns elements to a static collection of
 * {@link BoundedWindow BoundedWindows}.
 */
class StaticWindowFn<W extends BoundedWindow> extends NonMergingWindowFn<Object, W> {
  /**
   * Create a new {@link StaticWindowFn} that assigns elements to the provided
   * {@link BoundedWindow BoundedWindows}.
   */
  public static <W extends BoundedWindow> StaticWindowFn<W> of(
      Coder<W> coder,
      W window,
      W... windows) {
    return of(coder, ImmutableSet.<W>builder().add(window).add(windows).build());
  }

  /**
   * Create a new {@link StaticWindowFn} that assigns elements to the provided
   * {@link BoundedWindow BoundedWindows}.
   */
  public static <W extends BoundedWindow> StaticWindowFn<W> of(
      Coder<W> coder,
      Iterable<W> windows) {
    return new StaticWindowFn<>(coder, windows);
  }

  /**
   * A {@link Coder} capable of encoding and decoding all of the windows.
   */
  private final Coder<W> windowCoder;

  /**
   * The encoded representation of the windows of this {@link StaticWindowFn}.
   *
   * <p>Required because {@link BoundedWindow} is not serializable.
   */
  private final ImmutableCollection<byte[]> encodedWindows;

  /**
   * A cached view of the windows. See also {@link #getWindows()}.
   */
  private transient Collection<W> decodedWindows;

  private StaticWindowFn(Coder<W> coder, Iterable<W> windows) {
    this.windowCoder = coder;
    ImmutableSet.Builder<byte[]> encodedWindowsBuilder = ImmutableSet.builder();
    for (W w : windows) {
      try {
        encodedWindowsBuilder.add(CoderUtils.encodeToByteArray(coder, w));
      } catch (CoderException e) {
        throw new IllegalArgumentException(
            "Could not encode all windows with the provided window coder",
            e);
      }
    }
    this.encodedWindows = encodedWindowsBuilder.build();
  }

  /**
   * Decode and cache the {@link BoundedWindow BoundedWindows} in this {@link StaticWindowFn}.
   */
  private Collection<W> getWindows() {
    if (decodedWindows == null) {
      ImmutableSet.Builder<W> builder = ImmutableSet.builder();
      for (byte[] encodedWindow : encodedWindows) {
        try {
          builder.add(CoderUtils.decodeFromByteArray(windowCoder, encodedWindow));
        } catch (CoderException e) {
          throw new IllegalArgumentException(
              "Couldn't decode all provided windows with provided coder",
              e);
        }
      }
      decodedWindows = builder.build();
    }
    return decodedWindows;
  }

  @Override
  public Collection<W> assignWindows(AssignContext c) throws Exception {
    return getWindows();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return equals(other);
  }

  @Override
  public Coder<W> windowCoder() {
    return windowCoder;
  }

  @Override
  public W getSideInputWindow(BoundedWindow window) {
    checkArgument(getWindows().contains(window),
        "Tried to access main input window %s as a side input window in a StaticWindowFn, but "
            + "that window is not in the collection of windows.", window);
    return (W) window;
  }

  public boolean equals(Object other) {
    if (!(other instanceof StaticWindowFn)) {
      return false;
    }
    StaticWindowFn<?> that = (StaticWindowFn<?>) other;
    return Objects.equals(this.windowCoder, that.windowCoder)
        && this.getWindows().equals(that.getWindows());
  }

  public int hashCode() {
    return Objects.hash(windowCoder, decodedWindows);
  }
}
