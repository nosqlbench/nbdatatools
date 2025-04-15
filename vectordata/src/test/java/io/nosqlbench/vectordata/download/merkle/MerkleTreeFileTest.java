package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MerkleTreeFileTest {
    
    @TempDir
    Path tempDir;

    @Test
    void testCreateAndLoadWithDefaultParameters() throws IOException {
        // Create a test file
        Path sourceFile = createTestFile("test1.txt", 1024 * 1024); // 1MB file
        Path merkleFile = tempDir.resolve("test1.merkle");

        // Create and save Merkle tree
        MerkleTreeFile originalTree = MerkleTreeFile.create(sourceFile);
        originalTree.save(merkleFile);

        // Load the saved Merkle tree
        MerkleTreeFile loadedTree = MerkleTreeFile.load(merkleFile);

        // Verify the trees are identical
        assertTreesEqual(originalTree, loadedTree);
    }

    @Test
    void testCreateAndLoadWithCustomParameters() throws IOException {
        Path sourceFile = createTestFile("test2.txt", 2 * 1024 * 1024); // 2MB file
        Path merkleFile = tempDir.resolve("test2.merkle");

        // Create with custom section sizes
        MerkleTreeFile originalTree = MerkleTreeFile.create(sourceFile, 512, 8192);
        originalTree.save(merkleFile);

        MerkleTreeFile loadedTree = MerkleTreeFile.load(merkleFile);
        assertTreesEqual(originalTree, loadedTree);
    }

    @Test
    void testEmptyFile() throws IOException {
        Path sourceFile = createTestFile("empty.txt", 0);
        Path merkleFile = tempDir.resolve("empty.merkle");

        MerkleTreeFile tree = MerkleTreeFile.create(sourceFile);
        tree.save(merkleFile);

        MerkleTreeFile loadedTree = MerkleTreeFile.load(merkleFile);
        assertTreesEqual(tree, loadedTree);
    }

    @Test
    void testSmallFile() throws IOException {
        Path sourceFile = tempDir.resolve("small.txt");
        Files.writeString(sourceFile, "Hello, World!");
        Path merkleFile = tempDir.resolve("small.merkle");

        MerkleTreeFile tree = MerkleTreeFile.create(sourceFile);
        tree.save(merkleFile);

        MerkleTreeFile loadedTree = MerkleTreeFile.load(merkleFile);
        assertTreesEqual(tree, loadedTree);
    }

    @Test
    void testLargeFile() throws IOException {
        Path sourceFile = createTestFile("large.txt", 10 * 1024 * 1024); // 10MB file
        Path merkleFile = tempDir.resolve("large.merkle");

        MerkleTreeFile tree = MerkleTreeFile.create(sourceFile);
        tree.save(merkleFile);

        MerkleTreeFile loadedTree = MerkleTreeFile.load(merkleFile);
        assertTreesEqual(tree, loadedTree);
    }

    @Test
    void testGetters() throws IOException {
        Path sourceFile = createTestFile("getters_test.txt", 1024);
        MerkleTreeFile tree = MerkleTreeFile.create(sourceFile);

        // Test immutability of getters
        assertNotSame(tree.getTreeData(), tree.getTreeData());
        assertNotSame(tree.getLeafBoundaries(), tree.getLeafBoundaries());
        
        // Verify footer is not null
        assertNotNull(tree.getFooter());
        
        // Verify boundaries are sorted and increasing
        List<Long> boundaries = tree.getLeafBoundaries();
        assertThat(boundaries).isSorted();
        assertTrue(boundaries.get(0) == 0);
        assertTrue(boundaries.get(boundaries.size() - 1) == Files.size(sourceFile));
    }

    @Test
    void testFileModification() throws IOException {
        // Create original file and Merkle tree
        Path sourceFile = createTestFile("original.txt", 1024);
        Path merkleFile = tempDir.resolve("original.merkle");
        
        MerkleTreeFile originalTree = MerkleTreeFile.create(sourceFile);
        originalTree.save(merkleFile);
        
        // Modify the source file
        modifyFile(sourceFile);
        
        // Create new Merkle tree from modified file
        MerkleTreeFile modifiedTree = MerkleTreeFile.create(sourceFile);
        
        // Trees should be different
        assertFalse(treesAreEqual(originalTree, modifiedTree));
    }

    // Helper methods
    private Path createTestFile(String name, long size) throws IOException {
        Path file = tempDir.resolve(name);
        Random random = new Random(42); // Fixed seed for reproducibility
        byte[] buffer = new byte[8192];
        
        try (var os = Files.newOutputStream(file)) {
            while (size > 0) {
                random.nextBytes(buffer);
                int writeSize = (int) Math.min(buffer.length, size);
                os.write(buffer, 0, writeSize);
                size -= writeSize;
            }
        }
        return file;
    }

    private void modifyFile(Path file) throws IOException {
        byte[] content = Files.readAllBytes(file);
        // Modify a byte in the middle of the file
        if (content.length > 0) {
            content[content.length / 2] ^= 0xFF;
            Files.write(file, content);
        }
    }

    private void assertTreesEqual(MerkleTreeFile tree1, MerkleTreeFile tree2) {
        assertTrue(treesAreEqual(tree1, tree2), "Merkle trees are not equal");
    }

    private boolean treesAreEqual(MerkleTreeFile tree1, MerkleTreeFile tree2) {
        return tree1.getTreeData().equals(tree2.getTreeData()) &&
               tree1.getLeafBoundaries().equals(tree2.getLeafBoundaries()) &&
               tree1.getFooter().equals(tree2.getFooter());
    }
}