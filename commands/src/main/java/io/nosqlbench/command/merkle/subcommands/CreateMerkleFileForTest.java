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


import io.nosqlbench.vectordata.merklev2.MerkleRange;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import io.nosqlbench.vectordata.merklev2.MerkleRefBuildProgress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CreateMerkleFileForTest {
    public static void main(String[] args) throws Exception {
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
        
        // Create a MerkleData from the file data using merklev2
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(filePath);
        
        // Wait for completion and get the result
        MerkleDataImpl merkleData = progress.getFuture().get();
        
        // Save the MerkleData to a file
        Path merklePath = filePath.resolveSibling(filePath.getFileName() + ".mref");
        merkleData.save(merklePath);
        
        System.out.println("Merkle reference file created at: " + merklePath);
    }
}
