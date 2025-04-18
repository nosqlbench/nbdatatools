package io.nosqlbench.vectordata.merkle;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A test implementation of MerklePane for unit testing.
 */
public class TestMerklePane {
    private final MerkleTree tree;
    private final BitSet intactChunks;
    private final Map<Integer, ByteBuffer> submittedChunks = new HashMap<>();

    public TestMerklePane(MerkleTree tree, BitSet intactChunks) {
        this.tree = tree;
        this.intactChunks = intactChunks;
    }

    public MerkleTree getMerkleTree() {
        return tree;
    }

    public boolean isChunkIntact(int chunkIndex) {
        return intactChunks.get(chunkIndex);
    }

    public void submitChunk(int chunkIndex, ByteBuffer chunkData) {
        submittedChunks.put(chunkIndex, chunkData);
        intactChunks.set(chunkIndex);
    }

    public boolean verifyChunk(int chunkIndex) {
        return isChunkIntact(chunkIndex);
    }

    public Map<Integer, ByteBuffer> getSubmittedChunks() {
        return submittedChunks;
    }

    public int getSubmittedChunkCount() {
        return submittedChunks.size();
    }
}
