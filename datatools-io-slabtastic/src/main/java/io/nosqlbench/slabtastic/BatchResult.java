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

package io.nosqlbench.slabtastic;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

/// The result of a multi-batch read operation.
///
/// Results are ordered 1-to-1 with the submission order of the
/// corresponding {@link BatchRequest} list. Each slot is either present
/// (the record was found) or empty (the ordinal was not present in the
/// file, the namespace was unknown, or the ordinal was out of range).
///
/// @param results the ordered list of results, one per request
public record BatchResult(List<Optional<ByteBuffer>> results) {

    /// Returns the result at the given index.
    ///
    /// @param index the zero-based index corresponding to the request
    /// @return the record bytes, or empty if the ordinal was not found
    /// @throws IndexOutOfBoundsException if the index is out of range
    public Optional<ByteBuffer> get(int index) {
        return results.get(index);
    }

    /// Returns the number of results in this batch.
    /// @return the number of results
    public int size() {
        return results.size();
    }

    /// Returns true if every slot contains a present result.
    /// @return true if all slots are present
    public boolean isComplete() {
        for (Optional<ByteBuffer> r : results) {
            if (r.isEmpty()) return false;
        }
        return true;
    }

    /// Returns true if at least one slot is empty.
    /// @return true if any slot is empty
    public boolean hasPartialFailure() {
        for (Optional<ByteBuffer> r : results) {
            if (r.isEmpty()) return true;
        }
        return false;
    }

    /// Returns the number of empty slots.
    /// @return count of empty slots
    public int emptyCount() {
        int count = 0;
        for (Optional<ByteBuffer> r : results) {
            if (r.isEmpty()) count++;
        }
        return count;
    }

    /// Returns the number of present slots.
    /// @return count of present slots
    public int presentCount() {
        int count = 0;
        for (Optional<ByteBuffer> r : results) {
            if (r.isPresent()) count++;
        }
        return count;
    }
}
