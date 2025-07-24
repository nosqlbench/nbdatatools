package io.nosqlbench.vectordata.merklev2;

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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to debug MAFileChannel chunk validation issues
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelSimpleTest {

    @Test
    void testSingleChunkValidation(@TempDir Path tempDir) throws Exception {
        // Create small test file with just 1 chunk
        int fileSize = 512 * 1024; // 512KB file (should be 1 chunk)
        Path sourceFile = createTestFile(tempDir, "simple_test.bin", fileSize);
        
        // Serve the file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("simple_test_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("simple_test.bin");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/simple_test_" + uniqueId + "/simple_test.bin";
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleStatePath = tempDir.resolve("state.mrkl");
        
        System.out.println("=== Testing single chunk download and validation ===");
        MerkleShape shape = merkleRef.getShape();
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Chunk size: " + shape.getChunkSize() + " bytes");
        System.out.println("Total chunks: " + shape.getLeafCount());
        
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Check initial state
            MerkleState initialState = MerkleState.load(merkleStatePath);
            BitSet initialValid = initialState.getValidChunks();
            System.out.println("Initial valid chunks: " + initialValid);
            assertEquals(0, initialValid.cardinality(), "No chunks should be valid initially");
            initialState.close();
            
            // Read from chunk 0
            System.out.println("\nReading from chunk 0...");
            ByteBuffer buffer = ByteBuffer.allocate(100);
            Future<Integer> readFuture = channel.read(buffer, 0);
            int bytesRead = readFuture.get();
            assertTrue(bytesRead > 0, "Should read some bytes");
            System.out.println("Read " + bytesRead + " bytes");
            
            // Force flush and wait
            channel.force(true);
            Thread.sleep(200); // Give more time
            
            // Check if chunk 0 is now valid
            MerkleState afterRead = MerkleState.load(merkleStatePath);
            boolean chunk0Valid = afterRead.isValid(0);
            BitSet afterValid = afterRead.getValidChunks();
            System.out.println("Valid chunks after read: " + afterValid);
            System.out.println("Chunk 0 valid: " + chunk0Valid);
            
            afterRead.close();
            
            assertTrue(chunk0Valid, "Chunk 0 should be valid after download and verification");
        }
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
        merkleRef.close();
    }
    
    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(987654); // Fixed seed
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
}