package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CoderUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * A {@link WindowFn} that assigns all elements to a static collection of
 * {@link BoundedWindow BoundedWindows}. Side inputs windowed into static windows only support
 * main input windows in the provided collection of windows.
 */
public final class StaticWindows extends NonMergingWindowFn<Object, BoundedWindow> {
  private final Coder<BoundedWindow> coder;
  private final Collection<byte[]> encodedWindows;

  private final boolean onlyExisting;

  public StaticWindows(
      Coder<BoundedWindow> coder, Collection<byte[]> encodedWindows, boolean onlyExisting) {
    checkArgument(!encodedWindows.isEmpty(), "Windows may not be empty");
    this.coder = coder;
    this.encodedWindows = encodedWindows;
    this.onlyExisting = onlyExisting;
  }

  public static <W extends BoundedWindow> StaticWindows of(Coder<W> coder, Iterable<W> windows) {
    ImmutableSet.Builder<byte[]> windowsBuilder = ImmutableSet.builder();
    for (W window : windows) {
      try {
        windowsBuilder.add(CoderUtils.encodeToByteArray(coder, window));
      } catch (CoderException e) {
        throw new IllegalArgumentException(
            "Couldn't encode provided static windows with provided coder", e);
      }
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    StaticWindows windowFn = new StaticWindows((Coder) coder, windowsBuilder.build(), false);
    return windowFn;
  }

  public static <W extends BoundedWindow> StaticWindows of(Coder<W> coder, W window) {
    return of(coder, Collections.singleton(window));
  }

  public Collection<BoundedWindow> getWindows() {
    ImmutableList.Builder<BoundedWindow> windowsBuilder = ImmutableList.builder();
    for (byte[] encoded : encodedWindows) {
      try {
        windowsBuilder.add(CoderUtils.decodeFromByteArray(coder, encoded));
      } catch (CoderException e) {
        throw new IllegalArgumentException(
            "Windows could not be decoded with the provided window coder",
            e);
      }
    }
    return windowsBuilder.build();
  }

  public StaticWindows intoOnlyExisting() {
    return new StaticWindows(coder, encodedWindows, true);
  }

  @Override
  public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
    Collection<BoundedWindow> windows = getWindows();
    if (onlyExisting) {
      ImmutableSet.Builder<BoundedWindow> resultWindows = ImmutableSet.builder();
      for (BoundedWindow w : c.windows()) {
        if (windows.contains(w)) {
          resultWindows.add(w);
        } else {
          throw new IllegalArgumentException(
              "Tried to assign windows to an element that iso not already windowed into a provided "
                  + "window when onlyExisting is set to true");
        }
      }
      return resultWindows.build();
    } else {
      return windows;
    }
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    if (!(other instanceof StaticWindows)) {
      return false;
    }
    StaticWindows that = (StaticWindows) other;
    return Objects.equals(this.encodedWindows, that.encodedWindows);
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    return coder;
  }

  @Override
  public BoundedWindow getSideInputWindow(BoundedWindow window) {
    checkArgument(getWindows().contains(window),
        "StaticWindows only supports side input windows for main input windows that it contains");
    return window;
  }

  @Override
  public boolean assignsToSingleWindow() {
    return encodedWindows.size() == 1;
  }
}
