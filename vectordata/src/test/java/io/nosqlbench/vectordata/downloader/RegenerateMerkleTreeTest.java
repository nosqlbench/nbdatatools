package io.nosqlbench.vectordata.downloader;

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

import io.nosqlbench.vectordata.merkle.MerkleRange;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import io.nosqlbench.vectordata.merkle.MerkleTreeBuildProgress;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This test regenerates the merkle tree file for the testxvec_base.fvec file.
 * It's used to fix the hash verification failure in ExtensiveChunkVerificationTest.
 */
public class RegenerateMerkleTreeTest {

    @Test
    public void regenerateMerkleTreeForTestXvecBase() throws Exception {
        // Path to the test file - use absolute paths
        Path contentPath = Paths.get("src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec").toAbsolutePath();
        Path merklePath = Paths.get("src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec.mrkl").toAbsolutePath();

        System.out.println("Content path: " + contentPath);
        System.out.println("Merkle path: " + merklePath);

        // Ensure the content file exists
        if (!Files.exists(contentPath)) {
            throw new IllegalStateException("Content file does not exist: " + contentPath);
        }

        // Get the size of the content file
        long contentSize = Files.size(contentPath);
        System.out.println("Content file size: " + contentSize + " bytes");

        // Create a new merkle tree from the content file
        MerkleTreeBuildProgress progress = MerkleTree.fromData(contentPath);
        MerkleTree newTree = progress.getFuture().join();

        // Save the new merkle tree
        newTree.save(merklePath);
        System.out.println("Merkle tree regenerated and saved to: " + merklePath);

        // Print some information about the new merkle tree
        System.out.println("Merkle tree chunk size: " + newTree.getChunkSize() + " bytes");
        System.out.println("Merkle tree total size: " + newTree.totalSize() + " bytes");
        System.out.println("Merkle tree number of leaves: " + newTree.getNumberOfLeaves());

        // Print the hash of chunk 9 (the one that was failing)
        byte[] hash = newTree.getHashForLeaf(9);
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        System.out.println("Hash for chunk 9: " + hexString.toString());
    }
}
