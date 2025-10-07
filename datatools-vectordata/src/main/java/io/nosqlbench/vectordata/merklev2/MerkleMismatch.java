package io.nosqlbench.vectordata.merklev2;

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


import java.util.Objects;

/// Represents a mismatched chunk in the Merkle tree comparison
public class MerkleMismatch {
  private final int chunkIndex;
  private final long startInclusive;
  private final long length;

  public MerkleMismatch(int chunkIndex, long startInclusive, long length) {
    this.chunkIndex = chunkIndex;
    this.startInclusive = startInclusive;
    this.length = length;
  }

  public int chunkIndex() {
    return chunkIndex;
  }

  public long startInclusive() {
    return startInclusive;
  }

  public long length() {
    return length;
  }

  /// The end offset, exclusive, calculated as start + len
  /// @return The end offset, exclusive
  public long endExclusive() {
    return startInclusive + length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MerkleMismatch that = (MerkleMismatch) o;
    return chunkIndex == that.chunkIndex && startInclusive == that.startInclusive && length == that.length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkIndex, startInclusive, length);
  }

  @Override
  public String toString() {
    return "MerkleMismatch{" +
           "chunkIndex=" + chunkIndex +
           ", startInclusive=" + startInclusive +
           ", length=" + length +
           '}';
  }
}