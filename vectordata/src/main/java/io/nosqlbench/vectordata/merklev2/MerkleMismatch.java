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


/// Represents a mismatched chunk in the Merkle tree comparison
/// @param chunkIndex The index of the mismatched chunk
/// @param startInclusive Starting byte offset of the chunk
/// @param length Length of the chunk in bytes
public record MerkleMismatch(int chunkIndex, long startInclusive, long length) {
  /// The end offset, exclusive, calculated as start + len
  /// @return The end offset, exclusive
  public long endExclusive() {
    return startInclusive + length;
  }
}