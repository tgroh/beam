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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.beam.runners.direct.InProcessPipelineRunner.InProcessPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.CountingSource.CounterMark;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Tests for basic {@link InProcessPipelineRunner} functionality.
 */
@RunWith(JUnit4.class)
public class InProcessPipelineRunnerTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private Pipeline getPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(InProcessPipelineRunner.class);
    opts.as(InProcessPipelineOptions.class).setShutdownUnboundedProducersWithMaxWatermark(true);

    Pipeline p = Pipeline.create(opts);
    return p;
  }

  @Test
  public void wordCountShouldSucceed() throws Throwable {
    Pipeline p = getPipeline();

    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(MapElements.via(new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                return input;
              }
            }))
            .apply(Count.<String>perElement());
    PCollection<String> countStrs =
        counts.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(KV<String, Long> input) {
            String str = String.format("%s: %s", input.getKey(), input.getValue());
            return str;
          }
        }));

    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    InProcessPipelineResult result = ((InProcessPipelineResult) p.run());
    result.awaitCompletion();
  }

  @Test(timeout = 5000L)
  public void byteArrayCountShouldSucceed() {
    Pipeline p = getPipeline();

    SerializableFunction<Integer, byte[]> getBytes = new SerializableFunction<Integer, byte[]>() {
      @Override
      public byte[] apply(Integer input) {
        try {
          return CoderUtils.encodeToByteArray(VarIntCoder.of(), input);
        } catch (CoderException e) {
          fail("Unexpected Coder Exception " + e);
          throw new AssertionError("Unreachable");
        }
      }
    };
    TypeDescriptor<byte[]> td = new TypeDescriptor<byte[]>() {
    };
    PCollection<byte[]> foos =
        p.apply(Create.of(1, 1, 1, 2, 2, 3)).apply(MapElements.via(getBytes).withOutputType(td));
    PCollection<byte[]> msync =
        p.apply(Create.of(1, -2, -8, -16)).apply(MapElements.via(getBytes).withOutputType(td));
    PCollection<byte[]> bytes =
        PCollectionList.of(foos).and(msync).apply(Flatten.<byte[]>pCollections());
    PCollection<KV<byte[], Long>> counts = bytes.apply(Count.<byte[]>perElement());
    PCollection<KV<Integer, Long>> countsBackToString =
        counts.apply(MapElements.via(new SimpleFunction<KV<byte[], Long>, KV<Integer, Long>>() {
          @Override
          public KV<Integer, Long> apply(KV<byte[], Long> input) {
            try {
              return KV.of(CoderUtils.decodeFromByteArray(VarIntCoder.of(), input.getKey()),
                  input.getValue());
            } catch (CoderException e) {
              fail("Unexpected Coder Exception " + e);
              throw new AssertionError("Unreachable");
        }
      }
    }));

    Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder().put(1, 4L)
        .put(2, 2L)
        .put(3, 1L)
        .put(-2, 1L)
        .put(-8, 1L)
        .put(-16, 1L)
        .build();
    PAssert.thatMap(countsBackToString).isEqualTo(expected);
  }

  @Test
  public void unboundedReadStartsEmptyShouldSucceed() {
    Pipeline p = getPipeline();
    UnboundedSource<Long, ?> source = new DeferredCountingSource(1000L, 5L);
    PCollection<Long> longs = p.apply(Read.from(source))
        .apply(Window.<Long>into(FixedWindows.of(Duration.millis(5000L))));
    PCollection<Long> counts = longs.apply(Count.<Long>perElement())
        .apply(Values.<Long>create());
    counts.apply(MapElements.via(new SimpleFunction<Long, Void>() {
      @Override
      public Void apply(Long input) {
        assertThat(input, equalTo(1L));
        return null;
      }
    }));

    PCollection<Long> sum = longs.apply(Sum.longsGlobally().withoutDefaults());
    long expected = 0;
    for (long i = 0; i < 1000L; i++) {
      expected += i;
    }
    // For anonymous fn access
    final long reallyExpected = expected;
    sum.apply(MapElements.via(new SimpleFunction<Long, Void>() {
      @Override
      public Void apply(Long input) {
        assertThat(input, equalTo(reallyExpected));
        return null;
      }
    }));
    p.run();
  }

  @Test
  public void transformDisplayDataExceptionShouldFail() {
    DoFn<Integer, Integer> brokenDoFn = new DoFn<Integer, Integer>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {}

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        throw new RuntimeException("oh noes!");
      }
    };

    Pipeline p = getPipeline();
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(brokenDoFn));

    thrown.expectMessage(brokenDoFn.getClass().getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!")));
    p.run();
  }

  @Test
  public void pipelineOptionsDisplayDataExceptionShouldFail() {
    Object brokenValueType = new Object() {
      @JsonValue
      public int getValue () {
        return 42;
      }

      @Override
      public String toString() {
        throw new RuntimeException("oh noes!!");
      }
    };

    Pipeline p = getPipeline();
    p.getOptions().as(ObjectPipelineOptions.class).setValue(brokenValueType);

    p.apply(Create.of(1, 2, 3));

    thrown.expectMessage(PipelineOptions.class.getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!!")));
    p.run();
  }

  /** {@link PipelineOptions} to inject bad object implementations. */
  public interface ObjectPipelineOptions extends PipelineOptions {
    Object getValue();
    void setValue(Object value);
  }


  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenOutputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
            c.output(outputList);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenTerminateDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a bad equals() still fails
   * in the {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, byte[]>() {
          @Override public void processElement(ProcessContext c) {
            byte[] outputArray = new byte[]{0x1, 0x2, 0x3};
            c.output(outputArray);
            outputArray[0] = 0xa;
            c.output(outputArray);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates its input with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingInputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
            .withCoder(ListCoder.of(VarIntCoder.of())))
        .apply(ParDo.of(new DoFn<List<Integer>, Integer>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> inputList = c.element();
            inputList.set(0, 37);
            c.output(12);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an input with a bad equals() still fails
   * in the {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingInputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(new byte[]{0x1, 0x2, 0x3}, new byte[]{0x4, 0x5, 0x6}))
        .apply(ParDo.of(new DoFn<byte[], Integer>() {
          @Override public void processElement(ProcessContext c) {
            byte[] inputArray = c.element();
            inputArray[0] = 0xa;
            c.output(13);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  private static class DeferredCountingSource extends UnboundedSource<Long, DeferralMark> {
    private final UnboundedSource<Long, CounterMark> underlying;
    private final long maxElements;
    private final long numDeferrals;

    @SuppressWarnings("deprecation")
    private DeferredCountingSource(
        long maxElements, long numDeferrals) {
      this.underlying = CountingSource.unboundedWithTimestampFn(new ValueTimestampFn());
      this.maxElements = maxElements;
      this.numDeferrals = numDeferrals;
    }

    @Override
    public List<? extends UnboundedSource<Long, DeferralMark>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable DeferralMark checkpointMark) {
      return new DeferringReader(this,
          underlying.createReader(options,
              checkpointMark == null ? null : checkpointMark.counterMark),
          checkpointMark);
    }

    @Nullable
    @Override
    public Coder<DeferralMark> getCheckpointMarkCoder() {
      return AvroCoder.of(DeferralMark.class);
    }

    @Override
    public void validate() {
      underlying.validate();
    }

    @Override
    public Coder<Long> getDefaultOutputCoder() {
      return underlying.getDefaultOutputCoder();
    }
  }

  private static class DeferringReader extends UnboundedSource.UnboundedReader<Long> {
    private DeferredCountingSource source;
    private final UnboundedSource.UnboundedReader<Long> underlying;
    private long deferralCount;
    private long totalElements;

    DeferringReader(
        DeferredCountingSource source,
        UnboundedReader<Long> reader,
        @Nullable DeferralMark mark) {
      this.source = source;
      this.underlying = reader;
      if (mark != null) {
        deferralCount = mark.deferralCount;
        totalElements = mark.totalElements;
      } else {
        deferralCount = 0L;
        totalElements = 0L;
      }
    }

    @Override
    public boolean start() throws IOException {
      if (totalElements >= source.maxElements) {
        return false;
      }
      if (deferralCount > source.numDeferrals) {
        totalElements++;
        return underlying.start();
      }
      deferralCount++;
      return false;
    }

    @Override
    public boolean advance() throws IOException {
      if (totalElements >= source.maxElements) {
        return false;
      }
      totalElements++;
      return underlying.advance();
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return underlying.getCurrent();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return underlying.getCurrentTimestamp();
    }

    @Override
    public void close() throws IOException {
      underlying.close();
    }

    @Override
    public Instant getWatermark() {
      if (totalElements >= source.maxElements) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      return underlying.getWatermark();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new DeferralMark(deferralCount,
          totalElements, (CounterMark) underlying.getCheckpointMark());
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
      return source;
    }
  }

  @DefaultCoder(AvroCoder.class)
  private static class DeferralMark implements CheckpointMark {
    private final long deferralCount;
    private final long totalElements;
    @Nullable
    private final CounterMark counterMark;

    private DeferralMark(
        long numDeferrals, long numElements, CounterMark counterMark) {
      this.deferralCount = numDeferrals;
      this.totalElements = numElements;
      this.counterMark = counterMark;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
    }

    @SuppressWarnings("unused")
    private DeferralMark() {
      this(0L, 0L, null);
    }
  }

  private static class ValueTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }
}
