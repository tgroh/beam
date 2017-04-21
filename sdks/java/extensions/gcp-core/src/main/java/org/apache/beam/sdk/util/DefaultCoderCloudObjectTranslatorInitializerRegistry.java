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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.ValueOnlyWindowedValueCoder;

/** The default {@link CoderCloudObjectTranslatorRegistrar}. */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
public class DefaultCoderCloudObjectTranslatorInitializerRegistry
    implements CoderCloudObjectTranslatorRegistrar {

  private static final CloudObjectTranslator<GlobalWindow.Coder> GLOBAL_WINDOW_TRANSLATOR =
      CoderCloudObjectTranslators.globalWindow();
  private static final CloudObjectTranslator<IntervalWindowCoder> INTERVAL_WINDOW_TRANSLATOR =
      CoderCloudObjectTranslators.intervalWindow();
  private static final CloudObjectTranslator<LengthPrefixCoder<?>> LENGTH_PREFIX_TRANSLATOR =
      CoderCloudObjectTranslators.lengthPrefix();
  private static final CloudObjectTranslator<KvCoder<?, ?>> PAIR_TRANSLATOR =
      CoderCloudObjectTranslators.pair();
  private static final CloudObjectTranslator<IterableCoder<?>> STREAM_TRANSLATOR =
      CoderCloudObjectTranslators.stream();
  private static final CloudObjectTranslator<ValueOnlyWindowedValueCoder<?>>
      VALUE_ONLY_WINDOWED_VALUE_CODER_TRANSLATOR = CoderCloudObjectTranslators.wrapper();
  private static final CloudObjectTranslator<FullWindowedValueCoder<?>> WINDOWED_VALUE_TRANSLATOR =
      CoderCloudObjectTranslators.windowedValue();

  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> getJavaClasses() {
    Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> result = new HashMap<>();
    result.put(KvCoder.class, PAIR_TRANSLATOR);
    result.put(IterableCoder.class, STREAM_TRANSLATOR);
    result.put(LengthPrefixCoder.class, LENGTH_PREFIX_TRANSLATOR);
    result.put(GlobalWindow.Coder.class, GLOBAL_WINDOW_TRANSLATOR);
    result.put(IntervalWindow.getCoder().getClass(), INTERVAL_WINDOW_TRANSLATOR);
    result.put(
        WindowedValue.FullWindowedValueCoder.class, WINDOWED_VALUE_TRANSLATOR);
    result.put(
        WindowedValue.ValueOnlyWindowedValueCoder.class,
        VALUE_ONLY_WINDOWED_VALUE_CODER_TRANSLATOR);
    return Collections.unmodifiableMap(result);
  }

  @Override
  public Map<String, CloudObjectTranslator<? extends Coder>> getCloudObjectClasses() {
    Map<String, CloudObjectTranslator<? extends Coder>> result = new HashMap<>();
    result.put(CloudObjectKinds.KIND_PAIR, PAIR_TRANSLATOR);
    result.put(CloudObjectKinds.KIND_STREAM, STREAM_TRANSLATOR);
    result.put(CloudObjectKinds.KIND_LENGTH_PREFIX, LENGTH_PREFIX_TRANSLATOR);
    result.put(CloudObjectKinds.KIND_GLOBAL_WINDOW, GLOBAL_WINDOW_TRANSLATOR);
    result.put(CloudObjectKinds.KIND_INTERVAL_WINDOW, INTERVAL_WINDOW_TRANSLATOR);
    result.put(CloudObjectKinds.KIND_WINDOWED_VALUE, WINDOWED_VALUE_TRANSLATOR);
    result.put(
        WindowedValue.ValueOnlyWindowedValueCoder.class.getName(),
        VALUE_ONLY_WINDOWED_VALUE_CODER_TRANSLATOR);
    return Collections.unmodifiableMap(result);
  }
}
