package io.nosqlbench.nbvectors.jjq.contexts;

import java.util.concurrent.atomic.AtomicLong;

public class NBEnumContext implements NBIdEnumerator {
  private final String fieldName;

  public NBEnumContext(String fieldName) {
    this.fieldName = fieldName;
  }
  private AtomicLong value = new AtomicLong(0L);
  @Override
  public long getAsLong() {
    return value.getAndIncrement();
  }

  @Override
  public String toString() {
    return "Enum(" + this.fieldName + "):@" + value.get();
  }
}
