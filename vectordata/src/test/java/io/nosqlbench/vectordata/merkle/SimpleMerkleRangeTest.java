package io.nosqlbench.vectordata.merkle;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * A simple test for the MerkleTree.findMismatchedChunksInRange method.
 * This test doesn't use JUnit to avoid compilation issues.
 */
public class SimpleMerkleRangeTest {

    public static void main(String[] args) {
        System.out.println("Testing MerkleTree.findMismatchedChunksInRange...");
        
        try {
            testFindMismatchedChunksInRange();
            System.out.println("✅ Test passed!");
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testFindMismatchedChunksInRange() throws Exception {
        // Create test data
        int dataSize = 1024 * 1024; // 1MB
        int chunkSize = 4096; // 4KB chunks
        ByteBuffer data1 = ByteBuffer.allocate(dataSize);
        ByteBuffer data2 = ByteBuffer.allocate(dataSize);
        
        // Fill with random data
        Random random = new Random(42); // Use fixed seed for reproducibility
        random.nextBytes(data1.array());
        
        // Copy data1 to data2
        data2.put(data1.array());
        data1.position(0);
        data2.position(0);
        
        // Modify specific chunks in data2
        // We'll modify chunks 10, 20, 30, 40, and 50
        int[] modifiedChunks = {10, 20, 30, 40, 50};
        for (int chunkIndex : modifiedChunks) {
            int offset = chunkIndex * chunkSize;
            // Only modify if within bounds
            if (offset + chunkSize <= dataSize) {
                for (int i = 0; i < chunkSize; i++) {
                    data2.put(offset + i, (byte) (data2.get(offset + i) ^ 0xFF)); // Flip bits
                }
            }
        }
        
        // Create MerkleTrees
        MerkleRange fullRange = new MerkleRange(0, dataSize);
        MerkleTree tree1 = MerkleTree.fromData(data1, chunkSize, fullRange);
        MerkleTree tree2 = MerkleTree.fromData(data2, chunkSize, fullRange);
        
        // Test finding mismatches in the full range
        int[] mismatches = tree1.findMismatchedChunksInRange(tree2, 0, dataSize / chunkSize);
        if (mismatches.length != modifiedChunks.length) {
            throw new Exception("Expected " + modifiedChunks.length + " mismatches, but got " + mismatches.length);
        }
        Arrays.sort(modifiedChunks); // Sort for comparison
        if (!Arrays.equals(modifiedChunks, mismatches)) {
            throw new Exception("Mismatched chunks don't match expected chunks");
        }
        
        // Test finding mismatches in a partial range (chunks 15-45)
        int[] partialMismatches = tree1.findMismatchedChunksInRange(tree2, 15, 45);
        int[] expectedPartialMismatches = {20, 30, 40};
        if (!Arrays.equals(expectedPartialMismatches, partialMismatches)) {
            throw new Exception("Partial mismatches don't match expected chunks");
        }
        
        // Test finding mismatches in a range with no differences (chunks 0-5)
        int[] noMismatches = tree1.findMismatchedChunksInRange(tree2, 0, 5);
        if (noMismatches.length != 0) {
            throw new Exception("Expected 0 mismatches, but got " + noMismatches.length);
        }
        
        System.out.println("All tests passed!");
    }
}
