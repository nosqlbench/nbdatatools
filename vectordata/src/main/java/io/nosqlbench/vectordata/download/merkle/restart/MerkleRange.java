package io.nosqlbench.vectordata.download.merkle.restart;

/// Represents a range within a total data size
/// Used for both specifying initial ranges and computing chunk boundaries
public record MerkleRange(long start, long end) {
    public MerkleRange {
        if (start < 0 || end < start) {
            throw new IllegalArgumentException("Invalid range: [" + start + ", " + end + "]");
        }
    }

    /// Returns true if this range fully contains another range
    public boolean contains(MerkleRange other) {
        return start <= other.start && end >= other.end;
    }

    /// Returns true if this range overlaps with another range
    public boolean overlaps(MerkleRange other) {
        return !(end <= other.start || start >= other.end);
    }

    /// Returns the size of this range in bytes
    public long size() {
        return end - start;
    }

    /// Returns true if the given offset falls within this range
    public boolean contains(long offset) {
        return offset >= start && offset < end;
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