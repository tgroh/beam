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

package org.apache.beam.artifact.local;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;

/** An {@code ArtifactRetrievalService} which stages files to a local temp directory. */
public class LocalFileSystemArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements ArtifactRetrievalService {
  private static final long DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;

  public static LocalFileSystemArtifactRetrievalService forRootDirectory(File base) {
    return new LocalFileSystemArtifactRetrievalService(base);
  }

  private final LocalArtifactStagingLocation location;

  private LocalFileSystemArtifactRetrievalService(File rootDirectory) {
    this.location = LocalArtifactStagingLocation.forExistingDirectory(rootDirectory);
  }

  /**
   * Get the manifest of artifacts that can be retrieved by this {@link
   * LocalFileSystemArtifactRetrievalService}.
   */
  private ArtifactApi.Manifest getManifest() throws IOException {
    return ArtifactApi.Manifest.parseFrom(new FileInputStream(location.getManifestFile()));
  }

  @Override
  public final void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<GetManifestResponse> responseObserver) {
    try {
      responseObserver.onNext(GetManifestResponse.newBuilder().setManifest(getManifest()).build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL.withCause(e).asException());
    }
  }

  /** Get the artifact with the provided name as a sequence of bytes. */
  private Iterable<ByteBuffer> getArtifact(String name) throws IOException {
    File artifact = location.getArtifactFile(name);
    if (!artifact.exists()) {
      throw new FileNotFoundException(String.format("No such artifact %s", name));
    }
    FileChannel input = new FileInputStream(artifact).getChannel();
    List<ByteBuffer> chunks = new ArrayList<>();
    do {
      ByteBuffer buf = ByteBuffer.allocate((int) Math.min(DEFAULT_CHUNK_SIZE, input.size()));
      input.read(buf);
      buf.flip();
      chunks.add(buf);
    } while (chunks.size() * DEFAULT_CHUNK_SIZE < input.size());
    return chunks;
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    try {
      for (ByteBuffer artifactBytes : getArtifact(request.getName())) {
        responseObserver.onNext(ArtifactApi.ArtifactChunk.newBuilder()
            .setData(ByteString.copyFrom(artifactBytes))
            .build());
      }
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", request.getName()))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", request.getName()))
              .withCause(e)
              .asException());
    }
  }

  @Override
  public void close() throws Exception {}
}
