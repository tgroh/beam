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

package org.apache.beam.runners.local;

import com.google.auto.value.AutoValue;
import org.joda.time.Instant;

/**
 * A watermark hold.
 */
@AutoValue
public abstract class WatermarkHold {
  public static WatermarkHold of(StructuralKey<?> key, Instant hold) {
    return new AutoValue_WatermarkHold(key, hold);
  }

  public abstract StructuralKey<?> getKey();
  public abstract Instant getHold();
}
