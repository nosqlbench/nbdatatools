package io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.nio.ByteBuffer;
import java.util.Arrays;

/// A conjugate node represents a boolean conjugate like AND or OR
/// @param type the type of {@link ConjugateType}
/// @param values values to conjugate
public record ConjugateNode(ConjugateType type, PNode<?>... values)
    implements BBWriter<ConjugateNode>, PNode<ConjugateNode>
{

  ///  create a conjugate node
  /// @param b the byte buffer to decode the conjugate node from
  public ConjugateNode(ByteBuffer b) {
    this(ConjugateType.values()[b.get()], readValues(b));
  }

  private static PNode<?>[] readValues(ByteBuffer b) {
    byte count = b.get();
    PNode<?>[] elements = new PNode[count];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = PNode.fromBuffer(b);
    }
    return elements;
  }

  /// Encode this conjugate node into a byte buffer
  /// @param out the output buffer
  /// @return the output buffer, for method chaining
  @Override
  public ByteBuffer encode(ByteBuffer out) {
    out.put((byte) this.type.ordinal()).put((byte) this.values.length);
    for (PNode<?> element : values) {
      element.encode(out);
    }
    return out;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConjugateNode{");
    sb.append("type=").append(type);
    sb.append(", v=").append(values == null ? "null" : Arrays.asList(values).toString());
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ConjugateNode(ConjugateType type1, PNode<?>[] values1)))
      return false;

    return Arrays.equals(values, values1) && type == type1;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }
}
