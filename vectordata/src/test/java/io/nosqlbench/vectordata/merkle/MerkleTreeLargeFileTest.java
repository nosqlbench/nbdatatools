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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests saving and loading a Merkle tree for a large (100GB) data range without actual data.
 */
public class MerkleTreeLargeFileTest {

    @TempDir
    Path tempDir;

    @Test
    void testSaveAndLoadLargeTree() throws IOException {
        // Define a large total size (100 GB) and a reasonable chunk size (1 MB)
        long totalSize = 100L * 1024 * 1024 * 1024;
        long chunkSize = 1L * 1024 * 1024;
        // Create an empty Merkle tree for the specified size
        MerkleTree tree = MerkleTree.createEmpty(totalSize, chunkSize);
        // Save the tree to a temporary file
        Path file = tempDir.resolve("large_tree.mrkl");
        tree.save(file);
        // Load the tree from disk
        MerkleTree loaded = MerkleTree.load(file);
        // Verify metadata matches
        assertEquals(tree.getChunkSize(), loaded.getChunkSize(), "Chunk size should match");
        assertEquals(tree.totalSize(), loaded.totalSize(), "Total size should match");
        assertEquals(tree.getNumberOfLeaves(), loaded.getNumberOfLeaves(), "Number of leaves should match");
        // Metadata should match after loading
        // (Content hashes may be lazily computed or padded differently for empty trees)
    }
}