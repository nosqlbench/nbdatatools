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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

/// Utility class for comparing and reconciling files using Merkle trees.
///
/// This class provides methods to find differences between files using their
/// Merkle trees, enabling efficient partial file updates and verification.
///
public class MerkleTreeReconciler {
    private static final int HASH_SIZE = 32; // SHA-256 hash size

    /// Represents a section of data that needs to be reconciled.
    ///
    /// This class contains information about a file section that differs between
    /// the expected and actual files, including offsets and hash values.
    public static class ReconciliationTask {
        private final long startOffset;
        private final long endOffset;
        private final long size;
        private final byte[] expectedHash;
        private final byte[] actualHash;

        public ReconciliationTask(long startOffset, long endOffset, byte[] expectedHash, byte[] actualHash) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.size = endOffset - startOffset;
            this.expectedHash = expectedHash;
            this.actualHash = actualHash;
        }

        public long getStartOffset() { return startOffset; }
        public long getEndOffset() { return endOffset; }
        public long getSize() { return size; }
        public byte[] getExpectedHash() { return expectedHash; }
        public byte[] getActualHash() { return actualHash; }

        @Override
        public String toString() {
            return String.format("ReconciliationTask{offset=%d-%d, size=%d}",
                startOffset, endOffset, size);
        }
    }

    /// Compares two Merkle trees and returns a list of sections that differ.
    ///
    /// @param expectedTree the reference Merkle tree
    /// @param expectedOffsets the offsets buffer for the reference tree
    /// @param actualTree the tree to compare against the reference
    /// @param actualOffsets the offsets buffer for the actual tree
    /// @return List of ReconciliationTask for sections that need to be reconciled
    public static List<ReconciliationTask> findDifferences(
            ByteBuffer expectedTree,
            ByteBuffer expectedOffsets,
            ByteBuffer actualTree,
            ByteBuffer actualOffsets) {

        List<ReconciliationTask> tasks = new ArrayList<>();

        // Verify the trees have the same structure
        int expectedLeafNodes = (expectedOffsets.remaining() / 8) - 1;  // 8 bytes per long
        int actualLeafNodes = (actualOffsets.remaining() / 8) - 1;

        if (expectedLeafNodes != actualLeafNodes) {
            throw new IllegalArgumentException(
                "Merkle trees have different structures: " +
                "expected " + expectedLeafNodes + " leaves, " +
                "got " + actualLeafNodes);
        }

        // Compare leaf nodes first
        for (int i = 0; i < expectedLeafNodes; i++) {
            byte[] expectedHash = new byte[HASH_SIZE];
            byte[] actualHash = new byte[HASH_SIZE];

            expectedTree.get(expectedHash);
            actualTree.get(actualHash);

            if (!Arrays.equals(expectedHash, actualHash)) {
                long startOffset = expectedOffsets.getLong(i * 8);
                long endOffset = expectedOffsets.getLong((i + 1) * 8);

                tasks.add(new ReconciliationTask(
                    startOffset,
                    endOffset,
                    expectedHash,
                    actualHash
                ));
            }
        }

        return tasks;
    }

    /// Compares two files using their Merkle trees and returns reconciliation tasks.
    ///
    /// @param expectedPath path to the reference file
    /// @param actualPath path to the file to compare
    /// @param minSection minimum section size in bytes
    /// @param maxSection maximum section size in bytes
    /// @return List of ReconciliationTask for sections that need to be reconciled
    /// @throws IOException if there are file operation errors
    public static List<ReconciliationTask> compareFiles(
            Path expectedPath,
            Path actualPath,
            long minSection,
            long maxSection) throws IOException {

        // Build Merkle tree for expected file
        ByteBuffer expectedTree = MerkleTreeBuilder.buildMerkleTree(
            expectedPath, minSection, maxSection);

        // Get offsets for expected file
        List<Long> expectedOffsetsList = MerkleTreeRanger.computeMerkleOffsets(
            expectedPath.toFile().length(), minSection, maxSection);
        ByteBuffer expectedOffsets = ByteBuffer.allocate(expectedOffsetsList.size() * 8);
        expectedOffsetsList.forEach(expectedOffsets::putLong);
        expectedOffsets.flip();

        // Build Merkle tree for actual file
        ByteBuffer actualTree = MerkleTreeBuilder.buildMerkleTree(
            actualPath, minSection, maxSection);

        // Get offsets for actual file
        List<Long> actualOffsetsList = MerkleTreeRanger.computeMerkleOffsets(
            actualPath.toFile().length(), minSection, maxSection);
        ByteBuffer actualOffsets = ByteBuffer.allocate(actualOffsetsList.size() * 8);
        actualOffsetsList.forEach(actualOffsets::putLong);
        actualOffsets.flip();

        return findDifferences(expectedTree, expectedOffsets, actualTree, actualOffsets);
    }
}
