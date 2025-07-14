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


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

/**
 * This test class has been removed as it was testing methods that have been removed from MerkleTree.
 * The hashDataWithPrecomputedHash methods have been removed as per requirements.
 */
class MerkleTreePrecomputedHashTest {

    @TempDir
    Path tempDir;
    private Path merkleFilePath;
    private MerkleTree merkleTree;
    private static final int CHUNK_SIZE = 1024;
    private static final int TOTAL_SIZE = CHUNK_SIZE * 10; // 10 chunks
    private static final int HASH_SIZE = 32; // SHA-256 hash size

    @BeforeEach
    void setUp() throws IOException {
        merkleFilePath = tempDir.resolve("test.mrkl");
        merkleTree = MerkleTree.createEmpty(TOTAL_SIZE, CHUNK_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Ensure resources are released
        if (merkleTree != null) {
            merkleTree.save(merkleFilePath);
        }
    }

    // All tests have been removed as they were testing methods that have been removed from MerkleTree
}
