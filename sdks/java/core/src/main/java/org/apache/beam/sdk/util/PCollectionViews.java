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
package org.apache.beam.sdk.util;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Implementations of {@link PCollectionView} shared across the SDK.
 *
 * <p>For internal use only, subject to change.
 */
public class PCollectionViews {

  /**
   * Returns a {@code PCollectionView<T>} capable of processing elements encoded using the provided
   * {@link Coder} and windowed using the provided * {@link WindowingStrategy}.
   *
   * <p>If {@code hasDefault} is {@code true}, then the view will take on the value {@code
   * defaultValue} for any empty windows.
   */
  public static <T, W extends BoundedWindow> PCollectionView<T> singletonView(
      PCollection<T> pCollection, boolean hasDefault, @Nullable T defaultValue) {
    return new SimplePCollectionView<>(
        pCollection,
        new SingletonViewFn<>(hasDefault, defaultValue, pCollection.getCoder()),
        pCollection.getWindowingStrategy().getWindowFn().getDefaultWindowMappingFn());
  }

  /**
   * Returns a {@code PCollectionView<Iterable<T>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<Iterable<T>> iterableView(
      PCollection<T> pCollection) {
    return new SimplePCollectionView<>(
        pCollection,
        new IterableViewFn<T>(),
        pCollection.getWindowingStrategy().getWindowFn().getDefaultWindowMappingFn());
  }

  /**
   * Returns a {@code PCollectionView<List<T>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <T, W extends BoundedWindow> PCollectionView<List<T>> listView(
      PCollection<T> pCollection) {
    return new SimplePCollectionView<>(
        pCollection,
        new ListViewFn<T>(),
        pCollection.getWindowingStrategy().getWindowFn().getDefaultWindowMappingFn());
  }

  /**
   * Returns a {@code PCollectionView<Map<K, V>>} capable of processing elements encoded using the
   * provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, V>> mapView(
      PCollection<KV<K, V>> pCollection) {
    return new SimplePCollectionView<>(
        pCollection,
        new MapViewFn<K, V>(),
        pCollection.getWindowingStrategy().getWindowFn().getDefaultWindowMappingFn());
  }

  /**
   * Returns a {@code PCollectionView<Map<K, Iterable<V>>>} capable of processing elements encoded
   * using the provided {@link Coder} and windowed using the provided {@link WindowingStrategy}.
   */
  public static <K, V, W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>> multimapView(
      PCollection<KV<K, V>> pCollection) {
    return new SimplePCollectionView<>(
        pCollection,
        new MultimapViewFn<K, V>(),
        pCollection.getWindowingStrategy().getWindowFn().getDefaultWindowMappingFn());
  }

  /**
   * Implementation of conversion of singleton {@code Iterable<WindowedValue<T>>} to {@code T}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#singletonView}.
   */
  private static class SingletonViewFn<T> extends ViewFn<Iterable<WindowedValue<T>>, T> {
    @Nullable private byte[] encodedDefaultValue;
    @Nullable private transient T defaultValue;
    @Nullable private Coder<T> valueCoder;
    private boolean hasDefault;

    private SingletonViewFn(boolean hasDefault, T defaultValue, Coder<T> valueCoder) {
      this.hasDefault = hasDefault;
      this.defaultValue = defaultValue;
      this.valueCoder = valueCoder;
      if (hasDefault) {
        try {
          this.encodedDefaultValue = CoderUtils.encodeToByteArray(valueCoder, defaultValue);
        } catch (IOException e) {
          throw new RuntimeException("Unexpected IOException: ", e);
        }
      }
    }

    /**
     * Returns the default value that was specified.
     *
     * <p>For internal use only.
     *
     * @throws NoSuchElementException if no default was specified.
     */
    public T getDefaultValue() {
      if (!hasDefault) {
        throw new NoSuchElementException("Empty PCollection accessed as a singleton view.");
      }
      // Lazily decode the default value once
      synchronized (this) {
        if (encodedDefaultValue != null && defaultValue == null) {
          try {
            defaultValue = CoderUtils.decodeFromByteArray(valueCoder, encodedDefaultValue);
          } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException: ", e);
          }
        }
        return defaultValue;
      }
    }

    @Override
    public T apply(Iterable<WindowedValue<T>> contents) {
      try {
        return Iterables.getOnlyElement(contents).getValue();
      } catch (NoSuchElementException exc) {
        return getDefaultValue();
      } catch (IllegalArgumentException exc) {
        throw new IllegalArgumentException(
            "PCollection with more than one element "
                + "accessed as a singleton view.");
      }
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code Iterable<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#iterableView}.
   */
  private static class IterableViewFn<T>
      extends ViewFn<Iterable<WindowedValue<T>>, Iterable<T>> {

    @Override
    public Iterable<T> apply(Iterable<WindowedValue<T>> contents) {
      return Iterables.unmodifiableIterable(
          Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
        @SuppressWarnings("unchecked")
        @Override
        public T apply(WindowedValue<T> input) {
          return input.getValue();
        }
      }));
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<T>>} to {@code List<T>}.
   *
   * <p>For internal use only.
   *
   * <p>Instantiate via {@link PCollectionViews#listView}.
   */
  private static class ListViewFn<T> extends ViewFn<Iterable<WindowedValue<T>>, List<T>> {
    @Override
    public List<T> apply(Iterable<WindowedValue<T>> contents) {
      return ImmutableList.copyOf(
          Iterables.transform(contents, new Function<WindowedValue<T>, T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T apply(WindowedValue<T> input) {
              return input.getValue();
            }
          }));
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>>}
   * to {@code Map<K, Iterable<V>>}.
   */
  private static class MultimapViewFn<K, V>
      extends ViewFn<Iterable<WindowedValue<KV<K, V>>>, Map<K, Iterable<V>>> {

    @Override
    public Map<K, Iterable<V>> apply(Iterable<WindowedValue<KV<K, V>>> elements) {
      Multimap<K, V> multimap = HashMultimap.create();
      for (WindowedValue<KV<K, V>> elem : elements) {
        KV<K, V> kv = elem.getValue();
        multimap.put(kv.getKey(), kv.getValue());
      }
      // Safe covariant cast that Java cannot express without rawtypes, even with unchecked casts
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<K, Iterable<V>> resultMap = (Map) multimap.asMap();
      return Collections.unmodifiableMap(resultMap);
    }
  }

  /**
   * Implementation of conversion {@code Iterable<WindowedValue<KV<K, V>>} with one value per key
   * to {@code Map<K, V>}.
   */
  private static class MapViewFn<K, V>
      extends ViewFn<Iterable<WindowedValue<KV<K, V>>>, Map<K, V>> {
    /**
     * Input iterable must actually be {@code Iterable<WindowedValue<KV<K, V>>>}.
     */
    @Override
    public Map<K, V> apply(Iterable<WindowedValue<KV<K, V>>> elements) {
      Map<K, V> map = new HashMap<>();
      for (WindowedValue<KV<K, V>> elem : elements) {
        KV<K, V> kv = elem.getValue();
        if (map.containsKey(kv.getKey())) {
          throw new IllegalArgumentException("Duplicate values for " + kv.getKey());
        }
        map.put(kv.getKey(), kv.getValue());
      }
      return Collections.unmodifiableMap(map);
    }
  }

  /**
   * A base class for {@link PCollectionView} implementations, with additional type parameters
   * that are not visible at pipeline assembly time when the view is used as a side input.
   */
  private static class SimplePCollectionView<ElemT, ViewT, W extends BoundedWindow>
      extends PValueBase
      implements PCollectionView<ViewT> {
    /** A unique tag for the view, typed according to the elements underlying the view. */
    private TupleTag<Iterable<WindowedValue<ElemT>>> tag;

    /** The {@link WindowMappingFn} to map main input windows to the window in the view. */
    private WindowMappingFn<?> windowMappingFn;

    /** The coder for the elements underlying the view. */
    private Coder<Iterable<WindowedValue<ElemT>>> coder;

    /**
     * The typed {@link ViewFn} for this view.
     *
     * @deprecated Access to this variable from subclasses is temporary, for migrating away
     * from language-specific inspections.
     */
    @Deprecated
    protected ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn;

    /**
     * Call this constructor to initialize the fields for which this base class provides boilerplate
     * accessors.
     */
    // TODO: make private as soon as runners are ported off indicator subclasses
    protected SimplePCollectionView(
        PCollection<ElemT> pcollection,
        TupleTag<Iterable<WindowedValue<ElemT>>> tag,
        ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
        WindowMappingFn<?> windowMappingFn) {
      super(pcollection.getPipeline());
      this.tag = tag;
      this.windowMappingFn = windowMappingFn;
      this.viewFn = viewFn;
    }

    /**
     * Call this constructor to initialize the fields for which this base class provides boilerplate
     * accessors, with an auto-generated tag.
     */
    // TODO: make private as soon as runners are ported off indicator subclasses
    protected SimplePCollectionView(
        PCollection<ElemT> pcollection,
        ViewFn<Iterable<WindowedValue<ElemT>>, ViewT> viewFn,
        WindowMappingFn<?> windowMappingFn) {
      this(pcollection, new TupleTag<Iterable<WindowedValue<ElemT>>>(), viewFn, windowMappingFn);
    }

    /**
     * For serialization only. Do not use directly.
     */
    @SuppressWarnings("unused")  // used for serialization
    protected SimplePCollectionView() {
      super();
    }

    @Override
    public ViewFn<Iterable<WindowedValue<?>>, ViewT> getViewFn() {
      // Safe cast: it is required that the rest of the SDK maintain the invariant
      // that a PCollectionView is only provided an iterable for the elements of an
      // appropriately typed PCollection.
      @SuppressWarnings({"rawtypes", "unchecked"})
      ViewFn<Iterable<WindowedValue<?>>, ViewT> untypedViewFn = (ViewFn) viewFn;
      return untypedViewFn;
    }

    @Override
    public WindowMappingFn<?> getWindowMappingFn() {
      return windowMappingFn;
    }

    /**
     * Returns a unique {@link TupleTag} identifying this {@link PCollectionView}.
     *
     * <p>For internal use only by runner implementors.
     */
    @Override
    public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
      // Safe cast: It is required that the rest of the SDK maintain the invariant that
      // this tag is only used to access the contents of an appropriately typed underlying
      // PCollection
      @SuppressWarnings({"rawtypes", "unchecked"})
      TupleTag<Iterable<WindowedValue<?>>> untypedTag = (TupleTag) tag;
      return untypedTag;
    }

    @Override
    public Coder<Iterable<WindowedValue<?>>> getCoderInternal() {
      // Safe cast: It is required that the rest of the SDK only use this untyped coder
      // for the elements of an appropriately typed underlying PCollection.
      @SuppressWarnings({"rawtypes", "unchecked"})
      Coder<Iterable<WindowedValue<?>>> untypedCoder = (Coder) coder;
      return untypedCoder;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof PCollectionView)) {
        return false;
      }
      @SuppressWarnings("unchecked")
      PCollectionView<?> otherView = (PCollectionView<?>) other;
      return tag.equals(otherView.getTagInternal());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("tag", tag).toString();
    }
  }
}
