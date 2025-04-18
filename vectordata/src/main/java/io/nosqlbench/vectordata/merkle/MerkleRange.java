package io.nosqlbench.vectordata.merkle;

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


/// Represents a range within a total data size
/// Used for both specifying initial ranges and computing chunk boundaries
/// @param start The starting offset of the range
/// @param end The ending offset of the range
public record MerkleRange(long start, long end) {
    public MerkleRange {
        if (start < 0 || end < start) {
            throw new IllegalArgumentException("Invalid range: [" + start + ", " + end + "]");
        }
    }

    /// Returns true if this range fully contains another range
    /// @param other The range to check
    /// @return True if this range contains the other range
    public boolean contains(MerkleRange other) {
        return start <= other.start && end >= other.end;
    }

    /// Returns true if this range overlaps with another range
    /// @param other The range to check
    /// @return True if this range overlaps with the other range
    public boolean overlaps(MerkleRange other) {
        return !(end <= other.start || start >= other.end);
    }

    /// Returns the size of this range in bytes.
    ///
    /// @return The size of this range (end - start)
    public long size() {
        return end - start;
    }

    /// Returns true if the given offset falls within this range
    /// @param offset The offset to check
    /// @return True if the offset falls within this range
    public boolean contains(long offset) {
        return offset >= start && offset < end;
    }

    /// Returns the intersection of this range with another range, or null if there is no overlap
    /// @param other The range to intersect with
    /// @return The intersection of this range with the other range, or null if there is no overlap
    public MerkleRange intersection(MerkleRange other) {
        if (other == null) {
            return null;
        }

        long intersectionStart = Math.max(this.start, other.start);
        long intersectionEnd = Math.min(this.end, other.end);

        if (intersectionStart >= intersectionEnd) {
            return null;
        }

        return new MerkleRange(intersectionStart, intersectionEnd);
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MerkleRange range))
            return false;

        return end == range.end && start == range.start;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(start);
        result = 31 * result + Long.hashCode(end);
        return result;
    }
}
