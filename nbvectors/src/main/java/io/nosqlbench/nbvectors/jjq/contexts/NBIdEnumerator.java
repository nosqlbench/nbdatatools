package io.nosqlbench.nbvectors.jjq.contexts;

import java.util.function.LongSupplier;

public interface NBIdEnumerator extends LongSupplier {
  @Override
  long getAsLong();
}
