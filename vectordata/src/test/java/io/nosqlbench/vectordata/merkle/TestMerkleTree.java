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


/**
 * A simple implementation of MerkleTree for testing purposes.
 */
public class TestMerkleTree extends MerkleTree {
    private final long chunkSize;
    private final long totalSize;
    private final MerkleNode root;

    public TestMerkleTree(long chunkSize, long totalSize) {
        super(createRootNode(), chunkSize, totalSize, new MerkleRange(0, totalSize));
        this.chunkSize = chunkSize;
        this.totalSize = totalSize;
        this.root = super.root();
    }

    private static MerkleNode createRootNode() {
        // Create a simple root node with empty hash
        byte[] emptyHash = new byte[MerkleNode.HASH_SIZE];
        return new MerkleNode(0, 6, emptyHash, null, null);
    }

    @Override
    public int getChunkIndexForPosition(long position) {
        return (int) (position / chunkSize);
    }

    @Override
    public NodeBoundary getBoundariesForLeaf(int leafIndex) {
        long start = leafIndex * chunkSize;
        long end = Math.min((leafIndex + 1) * chunkSize, totalSize);
        return new NodeBoundary(start, end);
    }

    /**
     * Creates a test merkle tree with the specified number of chunks and chunk size.
     *
     * @param numChunks The number of chunks in the tree
     * @param chunkSize The size of each chunk in bytes
     * @return A test merkle tree
     */
    public static TestMerkleTree createTestTree(int numChunks, int chunkSize) {
        long totalSize = (long) numChunks * chunkSize;
        return new TestMerkleTree(chunkSize, totalSize);
    }
}
