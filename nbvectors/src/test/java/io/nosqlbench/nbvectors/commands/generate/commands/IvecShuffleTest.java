package io.nosqlbench.nbvectors.commands.generate.commands;

import io.nosqlbench.nbvectors.util.RandomGenerators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the IvecShuffle command, which generates deterministically
 * shuffled sequences of integers and saves them in ivec format.
 */
public class IvecShuffleTest {

    @TempDir
    Path tempDir;

    /**
     * Test basic functionality - creates a shuffled ivec file with correct content
     */
    @Test
    void testBasicGeneration() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("basic_test.ivec");
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", "100",
            "--seed", "42"
        );
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(outputPath), "Output file should be created");
        
        // Read and verify file content
        List<Integer> values = readIvecFile(outputPath);
        
        // Check that we have exactly the expected number of values
        assertEquals(100, values.size(), "Should generate exactly 100 values");
        
        // Check that all values in the range 0-99 are present
        Set<Integer> uniqueValues = new HashSet<>(values);
        assertEquals(100, uniqueValues.size(), "All values should be unique");
        for (int i = 0; i < 100; i++) {
            assertTrue(uniqueValues.contains(i), "Value " + i + " should be present");
        }
        
        // Check that the values are not in sequential order (shuffled)
        List<Integer> orderedList = IntStream.range(0, 100).boxed().collect(Collectors.toList());
        assertNotEquals(orderedList, values, "Values should not be in sequential order");
    }
    
    /**
     * Test that using the same seed produces identical shuffled sequences
     */
    @Test
    void testDeterministicShuffle() throws IOException {
        // Arrange
        Path outputPath1 = tempDir.resolve("deterministic_1.ivec");
        Path outputPath2 = tempDir.resolve("deterministic_2.ivec");
        long seed = 12345;
        
        // Act - Generate two files with the same seed
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath1.toString(),
            "--interval", "50",
            "--seed", String.valueOf(seed)
        );
        
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath2.toString(),
            "--interval", "50",
            "--seed", String.valueOf(seed)
        );
        
        // Assert
        List<Integer> values1 = readIvecFile(outputPath1);
        List<Integer> values2 = readIvecFile(outputPath2);
        
        assertThat(values1).isEqualTo(values2)
            .withFailMessage("Using the same seed should produce identical shuffled sequences");
    }
    
    /**
     * Test that using different seeds produces different shuffled sequences
     */
    @Test
    void testDifferentSeedsProduceDifferentSequences() throws IOException {
        // Arrange
        Path outputPath1 = tempDir.resolve("seed_1.ivec");
        Path outputPath2 = tempDir.resolve("seed_2.ivec");
        
        // Act - Generate two files with different seeds
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath1.toString(),
            "--interval", "100",
            "--seed", "1000"
        );
        
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath2.toString(),
            "--interval", "100",
            "--seed", "2000"
        );
        
        // Assert
        List<Integer> values1 = readIvecFile(outputPath1);
        List<Integer> values2 = readIvecFile(outputPath2);
        
        assertThat(values1).isNotEqualTo(values2)
            .withFailMessage("Different seeds should produce different shuffled sequences");
    }
    
    /**
     * Test that the command refuses to overwrite existing files without the force flag
     */
    @Test
    void testOverwriteProtection() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("protected.ivec");
        
        // First execution to create the file
        int firstExitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", "10",
            "--seed", "42"
        );
        
        assertEquals(0, firstExitCode, "First execution should succeed");
        assertTrue(Files.exists(outputPath), "File should be created");
        
        // Act - Try to overwrite without force flag
        int secondExitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", "10",
            "--seed", "42"
        );
        
        // Assert
        assertEquals(1, secondExitCode, "Should return exit code 1 when file exists without force flag");
    }
    
    /**
     * Test that the force flag allows overwriting existing files
     */
    @Test
    void testForceOverwrite() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("forced.ivec");
        
        // First execution
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", "10",
            "--seed", "42"
        );
        
        List<Integer> firstValues = readIvecFile(outputPath);
        
        // Act - Second execution with force flag and different seed
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", "10",
            "--seed", "999",
            "--force"
        );
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully with force flag");
        List<Integer> secondValues = readIvecFile(outputPath);
        assertThat(secondValues).isNotEqualTo(firstValues)
            .withFailMessage("File should be overwritten with new content");
    }
    
    /**
     * Test that the command creates necessary parent directories
     */
    @Test
    void testParentDirectoryCreation() {
        // Arrange
        Path nestedPath = tempDir.resolve("nested/directories/output.ivec");
        assertFalse(Files.exists(nestedPath.getParent()), "Parent directories should not exist yet");
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", nestedPath.toString(),
            "--interval", "5",
            "--seed", "42"
        );
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully");
        assertTrue(Files.exists(nestedPath), "Output file should be created");
        assertTrue(Files.exists(nestedPath.getParent()), "Parent directories should be created");
    }
    
    /**
     * Test the new handling of files with no parent directory
     */
    @Test
    void testFileWithNoParentDirectory() throws IOException {
        // Arrange - Create a file in the current directory with no parent path
        Path outputPath = Path.of("no_parent_dir_test.ivec");
        try {
            // Act
            int exitCode = new CommandLine(new IvecShuffle()).execute(
                "--output", outputPath.toString(),
                "--interval", "10",
                "--seed", "42"
            );
            
            // Assert
            assertEquals(0, exitCode, "Command should execute successfully with no parent directory");
            assertTrue(Files.exists(outputPath), "Output file should be created");
            
            // Verify file content
            List<Integer> values = readIvecFile(outputPath);
            assertEquals(10, values.size(), "Should generate exactly 10 values");
        } finally {
            // Clean up - delete the test file
            Files.deleteIfExists(outputPath);
        }
    }
    
    /**
     * Test handling of very large intervals
     */
    @Test
    void testLargeIntervalBoundaryCheck() {
        // Arrange
        Path outputPath = tempDir.resolve("large_interval.ivec");
        
        // Act - Try to generate with an interval that exceeds Integer.MAX_VALUE
        String largeInterval = String.valueOf(Integer.MAX_VALUE + 1L);
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", largeInterval,
            "--seed", "42"
        );
        
        // Assert
        assertEquals(2, exitCode, "Should return error exit code for too large interval");
        assertFalse(Files.exists(outputPath), "No output file should be created for invalid interval");
    }
    
    /**
     * Test interval at the maximum valid size
     */
    @Test
    void testValidMaximumInterval() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("max_valid_interval.ivec");
        int validLargeInterval = 1000; // Large enough to test but not too large for test performance
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", String.valueOf(validLargeInterval),
            "--seed", "42"
        );
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully with valid interval");
        assertTrue(Files.exists(outputPath), "Output file should be created");
        
        // Verify file contains correct number of entries
        List<Integer> values = readIvecFile(outputPath);
        assertEquals(validLargeInterval, values.size(), "Should generate exactly the requested number of values");
    }
    
    /**
     * Test different PRNG algorithms
     */
    @Test
    void testDifferentAlgorithms() throws IOException {
        // Arrange
        Path outputPath1 = tempDir.resolve("algo_xoshiro.ivec");
        Path outputPath2 = tempDir.resolve("algo_mt.ivec");
        
        // Act - Generate files with different algorithms but same seed
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath1.toString(),
            "--interval", "100",
            "--seed", "1234",
            "--algorithm", RandomGenerators.Algorithm.XO_SHI_RO_256_PP.name()
        );
        
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath2.toString(),
            "--interval", "100",
            "--seed", "1234",
            "--algorithm", RandomGenerators.Algorithm.MT.name()
        );
        
        // Assert - Different algorithms should produce different shuffles with same seed
        List<Integer> values1 = readIvecFile(outputPath1);
        List<Integer> values2 = readIvecFile(outputPath2);
        
        assertThat(values1).isNotEqualTo(values2)
            .withFailMessage("Different algorithms should produce different shuffled sequences");
    }
    
    /**
     * Test the path normalization feature
     */
    @Test
    void testPathNormalization() throws IOException {
        // Arrange - Create path with unnecessary ".." components
        Path baseDir = tempDir.resolve("base");
        Files.createDirectories(baseDir);
        Path complexPath = baseDir.resolve("../base/extra/../output.ivec");
        Path normalizedPath = baseDir.resolve("output.ivec");
        
        // Act
        int exitCode = new CommandLine(new IvecShuffle()).execute(
            "--output", complexPath.toString(),
            "--interval", "10",
            "--seed", "42"
        );
        
        // Assert
        assertEquals(0, exitCode, "Command should execute successfully with non-normalized path");
        assertTrue(Files.exists(normalizedPath), "Output file should be created at the normalized path");
        
        // Verify file content
        List<Integer> values = readIvecFile(normalizedPath);
        assertEquals(10, values.size(), "Should generate exactly 10 values");
    }
    
    /**
     * Statistical test - verify uniform distribution of values in output
     * 
     * This test verifies that no positional bias exists - each position
     * in the shuffled output should be able to contain any value with
     * equal probability.
     * 
     * Uses a chi-square goodness of fit test, which is more appropriate for
     * testing uniformity of distributions than direct frequency comparison.
     */
    @Test
    void testUniformDistribution() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("distribution_test.ivec");
        int interval = 20; // Small interval for test efficiency
        int samples = 100; // Increased sample size for better statistics
        
        // Create a frequency map for each position
        int[][] positionFrequency = new int[interval][interval];
        
        // Act - Generate multiple shuffles with different seeds
        for (int i = 0; i < samples; i++) {
            new CommandLine(new IvecShuffle()).execute(
                "--output", outputPath.toString(),
                "--interval", String.valueOf(interval),
                "--seed", String.valueOf(i),
                "--force"
            );
            
            List<Integer> values = readIvecFile(outputPath);
            
            // Record the frequency of each value at each position
            for (int position = 0; position < values.size(); position++) {
                int value = values.get(position);
                positionFrequency[position][value]++;
            }
        }
        
        // Calculate expected frequency
        double expectedFrequency = (double) samples / interval;
        
        // For chi-square test with 19 degrees of freedom (interval-1) and alpha=0.001,
        // the critical value is approximately 43.82. We'll use a slightly higher value
        // to give us a safe margin.
        double chiSquareCriticalValue = 45.0;
        
        // Count how many positions fail the chi-square test
        int failedPositions = 0;
        StringBuilder failureDetails = new StringBuilder();
        
        // For each position, perform a chi-square test
        for (int position = 0; position < interval; position++) {
            double chiSquare = 0.0;
            
            for (int value = 0; value < interval; value++) {
                double observed = positionFrequency[position][value];
                chiSquare += Math.pow(observed - expectedFrequency, 2) / expectedFrequency;
            }
            
            // If chi-square exceeds critical value, position fails the test
            if (chiSquare > chiSquareCriticalValue) {
                failedPositions++;
                failureDetails.append(String.format("Position %d: chi-square=%f\n", position, chiSquare));
            }
        }
        
        // Allow some positions to fail (statistical variance)
        // With alpha=0.001, we expect ~0.1% false positives, but we'll be more lenient
        int maxAllowedFailures = (int) Math.ceil(interval * 0.15); // Allow 15% to fail
        
        // Assert with more detailed error message if test fails
        if (failedPositions > maxAllowedFailures) {
            fail(String.format(
                "Too many positions (%d) failed the chi-square test (max allowed: %d)\n%s", 
                failedPositions, maxAllowedFailures, failureDetails.toString()
            ));
        }
    }
    
    /**
     * Statistical test - verify that consecutive runs are limited in shuffled output
     * 
     * A well-shuffled sequence should have very few consecutive ascending 
     * or descending runs of values.
     */
    @Test
    void testLimitedConsecutiveRuns() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("runs_test.ivec");
        int interval = 100;
        
        // Act
        new CommandLine(new IvecShuffle()).execute(
            "--output", outputPath.toString(),
            "--interval", String.valueOf(interval),
            "--seed", "12345"
        );
        
        List<Integer> values = readIvecFile(outputPath);
        
        // Count ascending and descending runs
        int ascendingRuns = 0;
        int descendingRuns = 0;
        
        for (int i = 0; i < values.size() - 1; i++) {
            if (values.get(i) + 1 == values.get(i + 1)) {
                ascendingRuns++;
            }
            if (values.get(i) - 1 == values.get(i + 1)) {
                descendingRuns++;
            }
        }
        
        // Assert - In a perfectly shuffled sequence, consecutive runs should be rare
        // For interval=100, expect fewer than ~5 ascending/descending runs by chance
        int maxExpectedRuns = 5;
        
        assertTrue(ascendingRuns <= maxExpectedRuns,
            "Too many ascending runs found (" + ascendingRuns + "), expected fewer than " + maxExpectedRuns);
        assertTrue(descendingRuns <= maxExpectedRuns,
            "Too many descending runs found (" + descendingRuns + "), expected fewer than " + maxExpectedRuns);
    }
    
    /**
     * Helper method to read ivec file content
     */
    private List<Integer> readIvecFile(Path path) throws IOException {
        List<Integer> values = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(path))) {
            while (dis.available() > 0) {
                int dimension = dis.readInt();
                assertEquals(1, dimension, "Each vector should have dimension of 1");
                int value = dis.readInt();
                values.add(value);
            }
        }
        
        return values;
    }
}
