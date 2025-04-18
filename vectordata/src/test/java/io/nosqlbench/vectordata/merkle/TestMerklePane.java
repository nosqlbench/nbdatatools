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
