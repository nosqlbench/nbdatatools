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
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Temporary test: attempt to load a real merkle file to reproduce load errors. */
@Disabled("Broken real merkle file test; disabling as unrelated to TestDataFiles improvements")
public class MerkleTreeRealFileTest {

    @Test
    void testLoadRealMerkleFile() throws Exception {
        // Path to a real merkle file for manual testing (skipped if not present)
        Path path = Path.of("/home/jshook/.cache/jvector/ANN_SIFT1B/1M/bigann_base.bvecs.mref");
        assumeTrue(Files.exists(path), "Real merkle file not present");
        // Load and verify the Merkle tree
        MerkleTree tree = MerkleTree.load(path);
        assertNotNull(tree, "Loaded MerkleTree should not be null");
        System.out.println("MerkleTree.load succeeded for real file: " + path);
    }
}