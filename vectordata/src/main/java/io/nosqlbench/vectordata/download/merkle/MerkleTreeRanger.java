package io.nosqlbench.vectordata.download.merkle;

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


import java.util.ArrayList;
import java.util.List;

/// Utility class for computing optimal file partitioning for Merkle trees.
///
/// This class provides methods to calculate partition boundaries for files
/// based on power-of-2 sizes, ensuring efficient Merkle tree construction.
///
public class MerkleTreeRanger {

    /// Returns the highest power of 2 that is less than or equal to x.
    /// Assumes x > 0.
    private static long highestPowerOf2(long x) {
        long p = 1;
        while (p <= x) {
            p <<= 1;
        }
        return p >> 1;
    }

    /// Returns the lowest power of 2 that is greater than or equal to x.
    /// Assumes x > 0.
    private static long lowestPowerOf2(long x) {
        long p = 1;
        while (p < x) {
            p <<= 1;
        }
        return p;
    }

    /// Computes the offset boundaries for file partitions (for a Merkle tree)
    /// such that the internal boundaries fall on multiples of a chosen power‐of‐2.
    /// The partition (or block) size is chosen as a power‑of‑2 that lies within [minSection, maxSection].
    ///
    /// @param fileSize   the total file size in bytes.
    /// @param minSection the minimum allowed section size (in bytes).
    /// @param maxSection the maximum allowed section size (in bytes).
    /// @return a List of offsets (in bytes), starting with 0 and ending with fileSize.
    ///         Adjacent offsets define one partition.
    /// @throws IllegalArgumentException if the input is invalid or no valid power‑of‑2 partition size exists.
    public static List<Long> computeMerkleOffsets(long fileSize, long minSection, long maxSection) {
        if (fileSize < 0 || minSection <= 0 || maxSection <= 0 || minSection > maxSection) {
            throw new IllegalArgumentException("Invalid input parameters.");
        }

        // Choose a partition size that is a power-of-2 and lies within [minSection, maxSection].
        long candidate = highestPowerOf2(maxSection);
        if (candidate < minSection) {
            candidate = lowestPowerOf2(minSection);
            if (candidate > maxSection) {
                throw new IllegalArgumentException("No power-of-2 partition size exists within the given bounds.");
            }
        }

        List<Long> offsets = new ArrayList<>();
        offsets.add(0L);

        // If the file is very small, return it as one partition.
        if (fileSize <= candidate) {
            offsets.add(fileSize);
            return offsets;
        }

        // Use the candidate block size to compute internal subdivision points.
        long currentOffset = candidate;
        while (currentOffset < fileSize) {
            offsets.add(currentOffset);
            currentOffset += candidate;
        }
        offsets.add(fileSize);

        // Check: if the final partition is smaller than minSection, merge it with the previous one.
        int n = offsets.size();
        if (offsets.get(n - 1) - offsets.get(n - 2) < minSection) {
            // Remove the second-to-last offset to merge the last partition
            offsets.remove(n - 2);
        }

        return offsets;
    }

    /// Example usage of the MerkleTreeRanger class.
    public static void main(String[] args) {
        // Example parameters:
        // - File size: 1 MB (1048576 bytes)
        // - Minimum section size: 1 KB (1024 bytes)
        // - Maximum section size: 64 KB (65536 bytes)
        long fileSize = 1_048_576;
        long minSection = 1024;
        long maxSection = 65536;

        List<Long> offsets = computeMerkleOffsets(fileSize, minSection, maxSection);
        System.out.println("Merkle Tree Offsets: " + offsets);
    }
}
