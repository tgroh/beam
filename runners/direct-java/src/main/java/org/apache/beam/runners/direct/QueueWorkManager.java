package org.apache.beam.runners.direct;

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;

import com.google.common.collect.ImmutableList;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link WorkManager} that uses a {@link Queue} as the underlying storage.
 */
public class QueueWorkManager implements WorkManager {
  private final ConcurrentLinkedQueue<Work> newWork;
  private final ConcurrentLinkedQueue<Work> returnedWork;

  public static QueueWorkManager create() {
    return new QueueWorkManager();
  }

  private QueueWorkManager() {
    newWork = new ConcurrentLinkedQueue<>();
    returnedWork = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void addWork(
      AppliedPTransform<?, ?, ?> transform,
      CommittedBundle<?> bundle) {
    newWork.offer(Work.of(transform, bundle));
  }

  @Override
  public void returnWork(
      AppliedPTransform<?, ?, ?> transform,
      CommittedBundle<?> bundle) {
    returnedWork.offer(Work.of(transform, bundle));
  }

  @Override
  public Iterable<Work> getWork() {
    ImmutableList.Builder<Work> resultBuilder = ImmutableList.builder();
    drain(resultBuilder, newWork);
    drain(resultBuilder, returnedWork);
    return resultBuilder.build();
  }

  /**
   * Drains the contents of a work queue into a builder.
   */
  private void drain(ImmutableList.Builder<Work> builder, Queue<Work> workQueue) {
    Work work = workQueue.poll();
    while (work != null) {
      builder.add(work);
      work = workQueue.poll();
    }
  }
}
