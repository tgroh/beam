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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest.ContentCase;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;

/**
 * An {@link ArtifactStagingServiceImplBase ArtifactStagingService} which stores the bytes of the
 * artifacts in memory.
 *
 * <p>Due to size limitations and other concerns, this should generally only be used to test clients
 * of the {@link ArtifactStagingServiceGrpc}.
 */
public class InMemoryArtifactStagerService extends ArtifactStagingServiceImplBase {
  private final ConcurrentMap<ArtifactMetadata, byte[]> artifactBytes;
  private Manifest manifest;

  private final AtomicInteger activePuts = new AtomicInteger();

  InMemoryArtifactStagerService() {
    artifactBytes = new ConcurrentHashMap<>();
  }

  @Override
  public StreamObserver<ArtifactApi.PutArtifactRequest> putArtifact(
      StreamObserver<ArtifactApi.PutArtifactResponse> responseObserver) {
    activePuts.getAndIncrement();
    BufferingObserver bufferingObserver = new BufferingObserver(responseObserver);
    return bufferingObserver;
  }

  @Override
  public void commitManifest(
      ArtifactApi.CommitManifestRequest request,
      StreamObserver<ArtifactApi.CommitManifestResponse> responseObserver) {
    this.manifest = request.getManifest();
    while (activePuts.get() != 0) {
      // Wait for existing calls to complete.
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
    }
    if (manifest.getArtifactCount() != artifactBytes.size()) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Different number of staged artifacts and artifacts in manifest.%n"
                          + "Manifest contents: %s%n"
                          + "Staged contents: %s",
                      manifest.getArtifactList(), artifactBytes.keySet()))
              .asException());
    } else {
      responseObserver.onNext(CommitManifestResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  public Map<ArtifactMetadata, byte[]> getStagedArtifacts() {
    return Collections.unmodifiableMap(artifactBytes);
  }

  public Manifest getManifest() {
    return manifest;
  }

  private class BufferingObserver implements StreamObserver<PutArtifactRequest> {
    private final StreamObserver<PutArtifactResponse> responseObserver;
    private ArtifactMetadata destination = null;
    private BufferWritingObserver writer = null;

    public BufferingObserver(StreamObserver<PutArtifactResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      if (writer == null) {
        checkArgument(value.getContentCase().equals(ContentCase.METADATA));
        writer = new BufferWritingObserver();
        destination = value.getMetadata();
      } else {
        writer.onNext(value);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (writer != null) {
        writer.onError(t);
      }
      onCompleted();
    }

    @Override
    public void onCompleted() {
      if (writer != null) {
        writer.onCompleted();
        try {
          artifactBytes.put(
              destination
                  .toBuilder()
                  .setMd5(
                      BaseEncoding.base64()
                          .encode(
                              MessageDigest.getInstance("MD5").digest(writer.stream.toByteArray())))
                  .build(),
              writer.stream.toByteArray());
        } catch (NoSuchAlgorithmException e) {
          throw new AssertionError("The Java Spec requires all JVMs to support MD5", e);
        }
      }
      activePuts.getAndDecrement();
      responseObserver.onNext(PutArtifactResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }

  private static class BufferWritingObserver implements StreamObserver<PutArtifactRequest> {
    private final ByteArrayOutputStream stream;

    BufferWritingObserver() {
      stream = new ByteArrayOutputStream();
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      try {
        stream.write(value.getData().getData().toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      onCompleted();
    }

    @Override
    public void onCompleted() {
    }
  }
}
