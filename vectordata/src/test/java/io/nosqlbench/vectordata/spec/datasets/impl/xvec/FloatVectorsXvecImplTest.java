package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for FloatVectorsXvecImpl to verify correct data can be read from fvec files.
 * 
 * This test class verifies that FloatVectorsXvecImpl can correctly:
 * - Read float vectors from fvec files
 * - Handle different vector dimensions
 * - Access vectors by index
 * - Return correct vector counts and metadata
 * - Work with both local file URLs and HTTP URLs
 */
@ExtendWith(JettyFileServerExtension.class)
public class FloatVectorsXvecImplTest {

    @TempDir
    Path tempDir;

    /**
     * Creates a test fvec file with known float vectors.
     * 
     * @param filePath The path where to create the file
     * @param vectors The float vectors to write to the file
     * @throws IOException If there's an error creating the file
     */
    private void createTestFvecFile(Path filePath, float[][] vectors) throws IOException {
        if (vectors.length == 0) {
            throw new IllegalArgumentException("Must provide at least one vector");
        }
        
        int dimensions = vectors[0].length;
        
        // Calculate the total size needed
        int recordSize = 4 + (dimensions * 4); // 4 bytes for dimension + dimensions * 4 bytes for floats
        ByteBuffer buffer = ByteBuffer.allocate(vectors.length * recordSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        // Write each vector
        for (float[] vector : vectors) {
            if (vector.length != dimensions) {
                throw new IllegalArgumentException("All vectors must have the same dimensions");
            }
            
            // Write dimension count
            buffer.putInt(dimensions);
            
            // Write vector data
            for (float value : vector) {
                buffer.putFloat(value);
            }
        }
        
        buffer.flip();
        Files.write(filePath, buffer.array());
    }

    /**
     * Creates a merkle tree file for the given fvec file.
     * 
     * @param filePath The path to the fvec file
     * @throws IOException If there's an error creating the merkle tree
     */
    private void createMerkleTreeFile(Path filePath) throws IOException {
        try {
            // Create merkle reference tree from data
            var progress = io.nosqlbench.vectordata.merklev2.MerkleRefFactory.fromData(filePath);
            var merkleRef = progress.getFuture().get();
            
            // Save as .mref file
            Path merklePath = filePath.resolveSibling(filePath.getFileName().toString() + ".mref");
            merkleRef.save(merklePath);
        } catch (Exception e) {
            throw new IOException("Failed to create merkle tree file", e);
        }
    }

    @Test
    public void testReadSimpleFloatVectors() throws IOException {
        // Create test vectors with known values
        float[][] testVectors = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {7.0f, 8.0f, 9.0f}
        };
        
        // Create test file
        Path fvecFile = tempDir.resolve("test_simple.fvecs");
        createTestFvecFile(fvecFile, testVectors);
        createMerkleTreeFile(fvecFile);
        
        // Test with local file URL
        MAFileChannel channel = MAFileChannel.create(
            fvecFile, fvecFile.resolveSibling(fvecFile.getFileName() + ".mref"), fvecFile.toUri().toString());
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(fvecFile), null, "fvecs");
        
        // Test basic metadata
        assertEquals(3, vectors.getCount(), "Should have 3 vectors");
        assertEquals(3, vectors.getVectorDimensions(), "Should have 3 dimensions");
        assertEquals(float.class, vectors.getDataType(), "Should be float type");
        
        // Test reading individual vectors
        float[] vector0 = vectors.get(0);
        assertArrayEquals(testVectors[0], vector0, 0.001f, "First vector should match");
        
        float[] vector1 = vectors.get(1);
        assertArrayEquals(testVectors[1], vector1, 0.001f, "Second vector should match");
        
        float[] vector2 = vectors.get(2);
        assertArrayEquals(testVectors[2], vector2, 0.001f, "Third vector should match");
        
        // Test reading range
        float[][] range = vectors.getRange(0, 2);
        assertEquals(2, range.length, "Range should contain 2 vectors");
        assertArrayEquals(testVectors[0], range[0], 0.001f, "First vector in range should match");
        assertArrayEquals(testVectors[1], range[1], 0.001f, "Second vector in range should match");
        
        // Test converting to list
        List<float[]> vectorList = vectors.toList();
        assertEquals(3, vectorList.size(), "List should contain 3 vectors");
        for (int i = 0; i < testVectors.length; i++) {
            assertArrayEquals(testVectors[i], vectorList.get(i), 0.001f, 
                "Vector " + i + " in list should match");
        }
        
        // Clean up
        channel.close();
    }

    @Test
    public void testReadHighDimensionalFloatVectors() throws IOException {
        // Create test vectors with higher dimensions
        int dimensions = 128;
        float[][] testVectors = new float[5][dimensions];
        
        // Fill with predictable patterns
        for (int i = 0; i < testVectors.length; i++) {
            for (int j = 0; j < dimensions; j++) {
                testVectors[i][j] = (float) (i * dimensions + j) / 100.0f;
            }
        }
        
        // Create test file
        Path fvecFile = tempDir.resolve("test_high_dim.fvecs");
        createTestFvecFile(fvecFile, testVectors);
        createMerkleTreeFile(fvecFile);
        
        // Test with local file URL
        MAFileChannel channel = MAFileChannel.create(
            fvecFile, fvecFile.resolveSibling(fvecFile.getFileName() + ".mref"), fvecFile.toUri().toString());
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(fvecFile), null, "fvecs");
        
        // Test basic metadata
        assertEquals(5, vectors.getCount(), "Should have 5 vectors");
        assertEquals(dimensions, vectors.getVectorDimensions(), "Should have correct dimensions");
        
        // Test reading vectors
        for (int i = 0; i < testVectors.length; i++) {
            float[] vector = vectors.get(i);
            assertArrayEquals(testVectors[i], vector, 0.001f, 
                "Vector " + i + " should match expected values");
        }
        
        // Test specific values
        float[] firstVector = vectors.get(0);
        assertEquals(0.0f, firstVector[0], 0.001f, "First element should be 0");
        assertEquals(1.27f, firstVector[127], 0.001f, "Last element should be 1.27");
        
        float[] lastVector = vectors.get(4);
        assertEquals(5.12f, lastVector[0], 0.001f, "First element of last vector should be 5.12");
        assertEquals(6.39f, lastVector[127], 0.001f, "Last element of last vector should be 6.39");
        
        // Clean up
        channel.close();
    }

    @Test
    public void testReadFloatVectorsWithSpecialValues() throws IOException {
        // Create test vectors with special float values
        float[][] testVectors = {
            {0.0f, -0.0f, 1.0f},
            {Float.MIN_VALUE, Float.MAX_VALUE, Float.MIN_NORMAL},
            {-1.0f, 0.5f, -0.5f},
            {1.23456789f, -9.87654321f, 3.14159265f}
        };
        
        // Create test file
        Path fvecFile = tempDir.resolve("test_special.fvecs");
        createTestFvecFile(fvecFile, testVectors);
        createMerkleTreeFile(fvecFile);
        
        // Test with local file URL
        MAFileChannel channel = MAFileChannel.create(
            fvecFile, fvecFile.resolveSibling(fvecFile.getFileName() + ".mref"), fvecFile.toUri().toString());
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(fvecFile), null, "fvecs");
        
        // Test reading vectors with special values
        for (int i = 0; i < testVectors.length; i++) {
            float[] vector = vectors.get(i);
            assertArrayEquals(testVectors[i], vector, 0.00001f,
                "Vector " + i + " should match expected special values");
        }
        
        // Test specific special values
        float[] vector0 = vectors.get(0);
        assertEquals(0.0f, vector0[0], "Zero should be preserved");
        assertEquals(-0.0f, vector0[1], "Negative zero should be preserved");
        assertEquals(1.0f, vector0[2], "One should be preserved");
        
        float[] vector1 = vectors.get(1);
        assertEquals(Float.MIN_VALUE, vector1[0], "MIN_VALUE should be preserved");
        assertEquals(Float.MAX_VALUE, vector1[1], "MAX_VALUE should be preserved");
        assertEquals(Float.MIN_NORMAL, vector1[2], "MIN_NORMAL should be preserved");
        
        // Clean up
        channel.close();
    }

    @Test
    public void testReadFloatVectorsWithHttpUrl() throws IOException {
        // Create test vectors
        float[][] testVectors = {
            {10.5f, 20.5f, 30.5f},
            {40.5f, 50.5f, 60.5f}
        };
        
        // Create files in server temp directory
        Path serverTempDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("fvectest");
        Files.createDirectories(serverTempDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverFile = serverTempDir.resolve("test_http_" + uniqueId + ".fvecs");
        createTestFvecFile(serverFile, testVectors);
        
        // Create local copy and merkle tree
        Path localFile = tempDir.resolve("local_http_test.fvecs");
        Files.copy(serverFile, localFile);
        createMerkleTreeFile(localFile);
        
        // Copy merkle tree to server
        Path localMerkle = localFile.resolveSibling(localFile.getFileName() + ".mref");
        Path serverMerkle = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        Files.copy(localMerkle, serverMerkle);
        
        // Test with HTTP URL
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String httpUrl = baseUrl.toString() + "temp/fvectest/test_http_" + uniqueId + ".fvecs";
        
        System.out.println("[DEBUG_LOG] Testing FloatVectorsXvecImpl with HTTP URL: " + httpUrl);
        
        MAFileChannel channel = MAFileChannel.create(
            localFile, localFile.resolveSibling(localFile.getFileName() + ".mref"), httpUrl);
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(serverFile), null, "fvecs");
        
        // Test basic metadata
        assertEquals(2, vectors.getCount(), "Should have 2 vectors");
        assertEquals(3, vectors.getVectorDimensions(), "Should have 3 dimensions");
        
        // Test reading vectors
        float[] vector0 = vectors.get(0);
        assertArrayEquals(testVectors[0], vector0, 0.001f, "First vector should match");
        
        float[] vector1 = vectors.get(1);
        assertArrayEquals(testVectors[1], vector1, 0.001f, "Second vector should match");
        
        // Clean up
        channel.close();
        
        // Clean up server files
        Files.deleteIfExists(serverMerkle);
        Files.deleteIfExists(serverFile);
        try {
            Files.deleteIfExists(serverTempDir);
        } catch (java.nio.file.DirectoryNotEmptyException e) {
            // Directory not empty, that's fine for a shared test directory
        }
    }

    @Test
    public void testReadSingleFloatVector() throws IOException {
        // Test with a single vector
        float[][] testVectors = {
            {1.1f, 2.2f, 3.3f, 4.4f, 5.5f}
        };
        
        // Create test file
        Path fvecFile = tempDir.resolve("test_single.fvecs");
        createTestFvecFile(fvecFile, testVectors);
        createMerkleTreeFile(fvecFile);
        
        // Test with local file URL
        MAFileChannel channel = MAFileChannel.create(
            fvecFile, fvecFile.resolveSibling(fvecFile.getFileName() + ".mref"), fvecFile.toUri().toString());
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(fvecFile), null, "fvecs");
        
        // Test basic metadata
        assertEquals(1, vectors.getCount(), "Should have 1 vector");
        assertEquals(5, vectors.getVectorDimensions(), "Should have 5 dimensions");
        
        // Test reading the single vector
        float[] vector = vectors.get(0);
        assertArrayEquals(testVectors[0], vector, 0.001f, "Vector should match");
        
        // Test range access
        float[][] range = vectors.getRange(0, 1);
        assertEquals(1, range.length, "Range should contain 1 vector");
        assertArrayEquals(testVectors[0], range[0], 0.001f, "Vector in range should match");
        
        // Test list conversion
        List<float[]> vectorList = vectors.toList();
        assertEquals(1, vectorList.size(), "List should contain 1 vector");
        assertArrayEquals(testVectors[0], vectorList.get(0), 0.001f, 
            "Vector in list should match");
        
        // Clean up
        channel.close();
    }

    @Test
    public void testReadLargeFloatVectors() throws IOException {
        // Create a larger set of vectors to test performance and correctness
        int vectorCount = 100;
        int dimensions = 64;
        float[][] testVectors = new float[vectorCount][dimensions];
        
        // Fill with predictable pattern
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dimensions; j++) {
                testVectors[i][j] = (float) Math.sin(i * 0.1 + j * 0.01);
            }
        }
        
        // Create test file
        Path fvecFile = tempDir.resolve("test_large.fvecs");
        createTestFvecFile(fvecFile, testVectors);
        createMerkleTreeFile(fvecFile);
        
        // Test with local file URL
        MAFileChannel channel = MAFileChannel.create(
            fvecFile, fvecFile.resolveSibling(fvecFile.getFileName() + ".mref"), fvecFile.toUri().toString());
        
        FloatVectorsXvecImpl vectors = new FloatVectorsXvecImpl(
            channel, Files.size(fvecFile), null, "fvecs");
        
        // Test basic metadata
        assertEquals(vectorCount, vectors.getCount(), "Should have correct vector count");
        assertEquals(dimensions, vectors.getVectorDimensions(), "Should have correct dimensions");
        
        // Test reading vectors at different positions
        float[] firstVector = vectors.get(0);
        assertArrayEquals(testVectors[0], firstVector, 0.001f, "First vector should match");
        
        float[] middleVector = vectors.get(vectorCount / 2);
        assertArrayEquals(testVectors[vectorCount / 2], middleVector, 0.001f, 
            "Middle vector should match");
        
        float[] lastVector = vectors.get(vectorCount - 1);
        assertArrayEquals(testVectors[vectorCount - 1], lastVector, 0.001f, 
            "Last vector should match");
        
        // Test reading a range
        int rangeStart = 10;
        int rangeEnd = 20;
        float[][] range = vectors.getRange(rangeStart, rangeEnd);
        assertEquals(rangeEnd - rangeStart, range.length, "Range should have correct size");
        
        for (int i = 0; i < range.length; i++) {
            assertArrayEquals(testVectors[rangeStart + i], range[i], 0.001f,
                "Vector " + (rangeStart + i) + " in range should match");
        }
        
        // Clean up
        channel.close();
    }
}