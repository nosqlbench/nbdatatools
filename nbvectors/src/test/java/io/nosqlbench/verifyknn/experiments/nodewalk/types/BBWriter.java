package io.nosqlbench.verifyknn.experiments.nodewalk.types;

import java.nio.ByteBuffer;

public interface BBWriter<T> {
  ByteBuffer encode(ByteBuffer out);
}
