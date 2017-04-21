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

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;

/**
 * The default {@link CoderCloudObjectTranslatorRegistrar}.
 */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
public class DefaultCoderCloudObjectTranslatorInitializerRegistry
    implements CoderCloudObjectTranslatorRegistrar {
  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> getInitializers() {
    Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> result = new HashMap<>();
    result.put(KvCoder.class, CoderCloudObjectTranslators.pair());
    result.put(IterableCoder.class, CoderCloudObjectTranslators.stream());
    result.put(LengthPrefixCoder.class, CoderCloudObjectTranslators.lengthPrefix());
    result.put(GlobalWindow.Coder.class, CoderCloudObjectTranslators.globalWindow());
    result.put(IntervalWindow.getCoder().getClass(), CoderCloudObjectTranslators.intervalWindow());
    result.put(
        WindowedValue.FullWindowedValueCoder.class, CoderCloudObjectTranslators.windowedValue());
    result.put(
        WindowedValue.ValueOnlyWindowedValueCoder.class, CoderCloudObjectTranslators.wrapper());
    return result;
  }
}
