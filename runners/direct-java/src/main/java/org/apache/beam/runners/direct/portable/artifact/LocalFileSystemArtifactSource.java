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

package org.apache.beam.runners.direct.portable.artifact;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@code ArtifactRetrievalService} which stages files to a local temp directory. */
public class LocalFileSystemArtifactSource implements ArtifactSource {
  private static final int DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemArtifactSource.class);

  public static LocalFileSystemArtifactSource forRootDirectory(File base) {
    return new LocalFileSystemArtifactSource(base);
  }

  private final LocalArtifactStagingLocation location;

  private LocalFileSystemArtifactSource(File rootDirectory) {
    this.location = LocalArtifactStagingLocation.forExistingDirectory(rootDirectory);
  }

  /** Get the artifact with the provided name as a sequence of bytes. */
  private ByteBuffer getArtifact(String name) throws IOException {
    File artifact = location.getArtifactFile(name);
    if (!artifact.exists()) {
      throw new FileNotFoundException(String.format("No such artifact %s", name));
    }
    FileChannel input = new FileInputStream(artifact).getChannel();
    return input.map(MapMode.READ_ONLY, 0L, input.size());
  }

  @Override
  public Manifest getManifest() throws IOException {
    try (FileInputStream manifestStream = new FileInputStream(location.getManifestFile())) {
      return ArtifactApi.Manifest.parseFrom(manifestStream);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("No %s in staging location %s", Manifest.class.getSimpleName(), location),
          e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    try {
      ByteBuffer artifact = getArtifact(name);
      do {
        responseObserver.onNext(
            ArtifactChunk.newBuilder()
                .setData(
                    ByteString.copyFrom(
                        artifact, Math.min(artifact.remaining(), DEFAULT_CHUNK_SIZE)))
                .build());
      } while (artifact.hasRemaining());
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", name))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(String.format("Could not retrieve artifact with name %s", name))
              .withCause(e)
              .asException());
    }
  }
}
