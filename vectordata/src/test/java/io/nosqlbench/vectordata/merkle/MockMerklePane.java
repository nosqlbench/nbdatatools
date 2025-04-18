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
 * A mock implementation of MerklePane for testing purposes.
 * This avoids the need to create a real MerklePane which would try to download files.
 */
// We're not extending MerklePane to avoid calling its constructor
public class MockMerklePane extends MerklePane {
    private final TestMerklePane testPane;
    private final Path dataPath;
    private final Path merklePath;
    private final MerkleTree merkleTree;
    private final BitSet intactChunks;
    private final MerkleBits merkleBits;

    public MockMerklePane(TestMerklePane testPane) {
        // We need to call super() but we'll override all methods
        super(null);
        this.testPane = testPane;
        this.dataPath = Path.of("mock-file-path");
        this.merklePath = Path.of("mock-merkle-path");
        this.merkleTree = testPane.getMerkleTree();
        this.intactChunks = new BitSet();
        this.merkleBits = new MerkleBits(intactChunks);
    }

    public MockMerklePane(TestMerklePane testPane, Path dataPath, Path merklePath) {
        // We need to call super() but we'll override all methods
        super(null);
        this.testPane = testPane;
        this.dataPath = dataPath;
        this.merklePath = merklePath;
        this.merkleTree = testPane.getMerkleTree();
        this.intactChunks = new BitSet();
        this.merkleBits = new MerkleBits(intactChunks);
    }

    @Override
    public MerkleTree getMerkleTree() {
        return merkleTree;
    }

    @Override
    public boolean isChunkIntact(int chunkIndex) {
        return testPane.isChunkIntact(chunkIndex);
    }

    @Override
    public void submitChunk(int chunkIndex, ByteBuffer chunkData) throws IOException {
        testPane.submitChunk(chunkIndex, chunkData);
    }

    @Override
    public boolean verifyChunk(int chunkIndex) throws IOException {
        return testPane.verifyChunk(chunkIndex);
    }

    @Override
    public Path getFilePath() {
        return dataPath;
    }

    @Override
    public Path getMerklePath() {
        return merklePath;
    }

    @Override
    public FileChannel getChannel() {
        return null; // Not used in tests
    }

    @Override
    public long getFileSize() {
        return testPane.getMerkleTree().totalSize();
    }

    @Override
    public BitSet getIntactChunks() {
        return new BitSet(); // Not used in tests
    }

    @Override
    public MerkleBits getMerkleBits() {
        return new MerkleBits(new BitSet()); // Not used in tests
    }

    @Override
    public ByteBuffer readChunk(int chunkIndex) throws IOException {
        // Create a mock chunk
        MerkleTree.NodeBoundary bounds = testPane.getMerkleTree().getBoundariesForLeaf(chunkIndex);
        int chunkSize = (int) (bounds.end() - bounds.start());
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        // Fill with a pattern based on the chunk index
        for (int i = 0; i < chunkSize; i++) {
            buffer.put((byte) ((chunkIndex * chunkSize + i) % 256));
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }
}
