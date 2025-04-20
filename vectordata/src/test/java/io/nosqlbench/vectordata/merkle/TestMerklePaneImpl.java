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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.BitSet;

/**
 * A complete mock implementation of MerklePane for testing purposes.
 * This is a standalone class that implements all the methods needed for testing.
 */
public class TestMerklePaneImpl {
    private final TestMerklePane testPane;
    private final Path dataPath;
    private final Path merklePath;
    private final MerkleTree merkleTree;
    private final BitSet intactChunks;

    public TestMerklePaneImpl(TestMerklePane testPane, Path dataPath, Path merklePath) {
        this.testPane = testPane;
        this.dataPath = dataPath;
        this.merklePath = merklePath;
        this.merkleTree = testPane.getMerkleTree();
        this.intactChunks = new BitSet();
    }

    public MerkleTree getMerkleTree() {
        return merkleTree;
    }

    public boolean isChunkIntact(int chunkIndex) {
        return testPane.isChunkIntact(chunkIndex);
    }

    public void submitChunk(int chunkIndex, ByteBuffer chunkData) {
        testPane.submitChunk(chunkIndex, chunkData);
    }

    public boolean verifyChunk(int chunkIndex) {
        return testPane.verifyChunk(chunkIndex);
    }

    public Path getFilePath() {
        return dataPath;
    }

    public Path getMerklePath() {
        return merklePath;
    }

    public FileChannel getChannel() {
        return null; // Not used in tests
    }

    public long getFileSize() {
        return merkleTree != null ? merkleTree.totalSize() : -1;
    }

    public BitSet getIntactChunks() {
        return intactChunks;
    }

    public ByteBuffer readChunk(int chunkIndex) throws IOException {
        // Create a mock chunk
        MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
        int chunkSize = (int) (bounds.end() - bounds.start());
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        // Fill with a pattern based on the chunk index
        for (int i = 0; i < chunkSize; i++) {
            buffer.put((byte) ((chunkIndex * chunkSize + i) % 256));
        }
        buffer.flip();
        return buffer;
    }
}
