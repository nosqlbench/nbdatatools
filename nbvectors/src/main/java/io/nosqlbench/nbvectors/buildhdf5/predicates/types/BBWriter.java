package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

import java.nio.ByteBuffer;

public interface BBWriter<T> {
  ByteBuffer encode(ByteBuffer out);
}
