package io.nosqlbench.command.merkle.subcommands;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CreateMerkleFileForTest {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: CreateMerkleFileForTest <file_path>");
            System.exit(1);
        }

        Path filePath = Paths.get(args[0]);

        System.out.println("Creating Merkle tree file for: " + filePath);
        
        // Read the file into a ByteBuffer
        long fileSize = java.nio.file.Files.size(filePath);
        ByteBuffer fileData = ByteBuffer.allocate((int)Math.min(fileSize, Integer.MAX_VALUE));
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            channel.read(fileData);
            fileData.flip();
        }
        
        // Create a MerkleTree from the file data
        MerkleTree merkleTree = MerkleTree.fromData(fileData);
        
        // Save the MerkleTree to a file
        Path merklePath = filePath.resolveSibling(filePath.getFileName() + ".mrkl");
        merkleTree.save(merklePath);
        
        System.out.println("Merkle tree file created at: " + merklePath);
        
        // Create a reference file by copying the merkle file
        Path refPath = filePath.resolveSibling(filePath.getFileName() + ".mref");
        java.nio.file.Files.copy(merklePath, refPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        
        System.out.println("Reference file created at: " + refPath);
    }
}
