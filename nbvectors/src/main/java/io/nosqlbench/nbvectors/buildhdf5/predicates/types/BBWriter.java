package io.nosqlbench.nbvectors.buildhdf5.predicates.types;

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
/// Any type implementing this interface knows how to encode itself into a byte buffer
/// @param <T> The type of self to write to the stream
public interface BBWriter<T> {
  /// write a representation of this to the stream
  /// @return the byte buffer, for method chaining
  /// @param out the target byte buffer
  ByteBuffer encode(ByteBuffer out);
}
