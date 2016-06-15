package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.WorkManager.Work;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.Iterables;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Iterator;

/**
 * Tests for {@link QueueWorkManager}.
 */
@RunWith(JUnit4.class)
public class QueueWorkManagerTest {
  private QueueWorkManager wm;

  private AppliedPTransform<?, ?, ?> transform;
  private CommittedBundle<Integer> createdBundle;
  private CommittedBundle<Integer> returnedBundle;

  @Before
  public void setup() {
    BundleFactory bundleFactory = ImmutableListBundleFactory.create();

    wm = QueueWorkManager.create();

    TestPipeline p = TestPipeline.create();
    PCollection<Integer> created = p.apply(Create.of(1, 2, 3));
    transform = created.getProducingTransformInternal();
    createdBundle = bundleFactory.createRootBundle(created).commit(Instant.now());
    returnedBundle = bundleFactory.createRootBundle(created).commit(Instant.now());
  }

  @Test
  public void addWorkThenGet() {
    wm.addWork(transform, createdBundle);
    Work work = Iterables.getOnlyElement(wm.getWork());

    assertThat(work.getBundle(), Matchers.<CommittedBundle<?>>equalTo(createdBundle));
    assertThat(work.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
  }

  @Test
  public void returnWorkThenGet() {
    wm.returnWork(transform, returnedBundle);
    Work work = Iterables.getOnlyElement(wm.getWork());

    assertThat(work.getBundle(), Matchers.<CommittedBundle<?>>equalTo(returnedBundle));
    assertThat(work.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
  }

  @Test
  public void addAndReturnWorkThenGet() {
    wm.addWork(transform, createdBundle);
    wm.returnWork(transform, returnedBundle);

    Iterable<Work> work = wm.getWork();
    assertThat(Iterables.size(work), equalTo(2));

    Iterator<Work> witer = work.iterator();
    Work first = witer.next();
    Work second = witer.next();
    assertThat(first.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
    assertThat(second.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
    if (first.getBundle().equals(createdBundle)) {
      assertThat(second.getBundle(), Matchers.<CommittedBundle<?>>equalTo(returnedBundle));
    } else if (first.getBundle().equals(returnedBundle)) {
      assertThat(second.getBundle(), Matchers.<CommittedBundle<?>>equalTo(createdBundle));
    } else {
      fail("Got an Unexpected bundle " + first.getBundle());
    }
  }

  @Test
  public void getWithoutAdd() {
    assertThat(wm.getWork(), emptyIterable());
  }

  @Test
  public void addThenMultipleGets() {
    wm.addWork(transform, createdBundle);
    wm.getWork();
    assertThat(wm.getWork(), Matchers.<Work>emptyIterable());
  }
}
