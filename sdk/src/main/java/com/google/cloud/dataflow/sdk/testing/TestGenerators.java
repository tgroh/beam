package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import java.util.List;

public final class TestGenerators {
  private TestGenerators() {
    // Do not instantiate.
  }
  
  private static <T> SimpleFunction<Long, T> inOrder(final TypeDescriptor<T> td, T... elements) {
    
  }

  public static class GeneratorFn<T> extends SimpleFunction<Long, T> {
    private final List<T> elements;
    private final AfterEndBehavior afterEnd;
    
    private GeneratorFn(List<T> elements, AfterEndBehavior afterEnd) {
      super();
      this.elements = elements;
      this.afterEnd = afterEnd;
    }

    public GeneratorFn<T> forever() {
      return new GeneratorFn<>(elements, AfterEndBehavior.LOOP);
    }
    
    public GeneratorFn<T> once() {
      return new GeneratorFn<>(elements, AfterEndBehavior.FINISH);
    }
    
    @Override
    public T apply(Long input) {
      if (input < elements.size()) {
        return elements.get(input.intValue());
      } else {
        switch (afterEnd) {
          case LOOP:
            int index = (int) (input % elements.size());
            return elements.get(index);
          case FINISH:
            return null;
          default:
            throw new AssertionError(
                "Unreachable code, due to exhaustive checking of AfterEndBehavior enum");
        }
      }
    } 
  }
  
  private static enum AfterEndBehavior {
    LOOP,
    FINISH;
  }
}
