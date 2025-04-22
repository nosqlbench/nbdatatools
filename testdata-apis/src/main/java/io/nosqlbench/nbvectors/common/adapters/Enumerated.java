package io.nosqlbench.nbvectors.common.adapters;

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


import java.util.Iterator;

/// An iterator which is also aware of its index
/// @param <T> the type of the elements
public interface Enumerated<T> extends Iterator<T> {

  /// get the last index
  /// @return the last index
  public long getLastIndex();

  /// Advance the iterator to the given index
  /// @param index the index to advance to
  public default void advanceTo(long index) {
    while (getLastIndex() < index - 1) {
      if (!hasNext()) {
        throw new RuntimeException("iterator can not advance to index " + index + ", because it has"
                                   + " no more elements at index " + getLastIndex());
      }
      next();
    }
  }

  /// Get the distance to the given index
  /// @param index the index to get the distance to
  /// @return the distance to the given index
  public default long distanceTo(long index) {
    return index - getLastIndex();
  }

}
