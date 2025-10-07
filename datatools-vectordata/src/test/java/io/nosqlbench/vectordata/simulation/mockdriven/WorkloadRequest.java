package io.nosqlbench.vectordata.simulation.mockdriven;

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

/// Represents a single read request in a workload pattern.
/// 
/// This class encapsulates the parameters for a read operation,
/// including the byte offset and length. It's used by workload
/// generators to define the sequence of operations that will
/// be performed during scheduler testing.
/// 
/// Each request represents a simulated application-level read
/// that the scheduler must satisfy by downloading the appropriate
/// chunks from the remote source.
public class WorkloadRequest {
    
    private final long offset;
    private final int length;
    
    /// Creates a new workload request.
    /// 
    /// @param offset Starting byte offset for the read
    /// @param length Number of bytes to read
    /// @throws IllegalArgumentException if parameters are invalid
    public WorkloadRequest(long offset, int length) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }
        
        this.offset = offset;
        this.length = length;
    }
    
    /// Gets the starting byte offset for this request.
    /// 
    /// @return Byte offset (0-based)
    public long getOffset() {
        return offset;
    }
    
    /// Gets the number of bytes to read.
    /// 
    /// @return Read length in bytes
    public int getLength() {
        return length;
    }
    
    /// Gets the ending byte position (exclusive) for this request.
    /// 
    /// @return End offset (offset + length)
    public long getEndOffset() {
        return offset + length;
    }
    
    /// Checks if this request overlaps with another request.
    /// 
    /// @param other The other request to check against
    /// @return true if the requests overlap
    public boolean overlaps(WorkloadRequest other) {
        return offset < other.getEndOffset() && getEndOffset() > other.getOffset();
    }
    
    /// Checks if this request is adjacent to another request.
    /// 
    /// Two requests are adjacent if they touch but don't overlap,
    /// which can be useful for request coalescing optimizations.
    /// 
    /// @param other The other request to check against
    /// @return true if the requests are adjacent
    public boolean isAdjacentTo(WorkloadRequest other) {
        return getEndOffset() == other.getOffset() || other.getEndOffset() == offset;
    }
    
    /// Checks if this request contains the specified offset.
    /// 
    /// @param byteOffset The byte offset to check
    /// @return true if the offset is within this request's range
    public boolean contains(long byteOffset) {
        return byteOffset >= offset && byteOffset < getEndOffset();
    }
    
    /// Creates a new request representing the union of this and another request.
    /// 
    /// The union covers the entire range from the minimum offset to the
    /// maximum end offset of both requests.
    /// 
    /// @param other The other request to union with
    /// @return A new request covering both ranges
    public WorkloadRequest union(WorkloadRequest other) {
        long minOffset = Math.min(offset, other.getOffset());
        long maxEndOffset = Math.max(getEndOffset(), other.getEndOffset());
        return new WorkloadRequest(minOffset, (int) (maxEndOffset - minOffset));
    }
    
    /// Creates a new request representing the intersection of this and another request.
    /// 
    /// @param other The other request to intersect with
    /// @return A new request covering only the overlapping range, or null if no overlap
    public WorkloadRequest intersection(WorkloadRequest other) {
        long startOffset = Math.max(offset, other.getOffset());
        long endOffset = Math.min(getEndOffset(), other.getEndOffset());
        
        if (startOffset >= endOffset) {
            return null; // No intersection
        }
        
        return new WorkloadRequest(startOffset, (int) (endOffset - startOffset));
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        WorkloadRequest that = (WorkloadRequest) o;
        return offset == that.offset && length == that.length;
    }
    
    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + length;
    }
    
    @Override
    public String toString() {
        return String.format("WorkloadRequest[%d:%d]", offset, length);
    }
}