package io.nosqlbench.nbvectors.commands.generate.commands;

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
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Statistical tests for the IvecShuffle command to verify the quality
 * of the shuffling algorithm beyond basic functionality tests.
 */
public class IvecShuffleStatisticalTest {

    @TempDir
    Path tempDir;

    /**
     * Test that the distribution of values is uniform using a chi-square test.
     * This verifies that all numbers appear with roughly equal frequency over multiple runs.
     */
    @Test
    void testUniformDistribution() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("uniform_test.ivec");
        int interval = 100;
        int runs = 200; // Increased number of runs for better statistical significance
        
        // Maps to count occurrences of each value in different positions
        Map<Integer, Map<Integer, Integer>> positionCounts = new HashMap<>();
        
        // Initialize the position counts map
        for (int position = 0; position < interval; position++) {
            positionCounts.put(position, new HashMap<>());
            for (int value = 0; value < interval; value++) {
                positionCounts.get(position).put(value, 0);
            }
        }
        
        // Act - Generate multiple shuffled sequences with different seeds
        for (int run = 0; run < runs; run++) {
            new CommandLine(new IvecShuffle()).execute(
                outputPath.toString(), "--interval", String.valueOf(interval), 
                "--seed", String.valueOf(run), "--force");
            
            List<Integer> values = readIvecFile(outputPath);
            
            // Count occurrences of each value at each position
            for (int position = 0; position < values.size(); position++) {
                int value = values.get(position);
                Map<Integer, Integer> countsForPosition = positionCounts.get(position);
                countsForPosition.put(value, countsForPosition.get(value) + 1);
            }
        }
        
        // Assert - Calculate chi-square statistic for each position
        // For a=0.001 (99.9% confidence) with df=99, chi-square critical value ≈ 149.4
        // Using a slightly higher value for better test stability
        double criticalValue = 160.0; 
        
        int failedPositions = 0;
        StringBuilder failureDetails = new StringBuilder();
        
        for (int position = 0; position < interval; position++) {
            Map<Integer, Integer> countsForPosition = positionCounts.get(position);
            double expected = (double) runs / interval; // Expected count for each value
            
            double chiSquare = countsForPosition.values().stream()
                .mapToDouble(count -> Math.pow(count - expected, 2) / expected)
                .sum();
            
            // Track failures for reporting but allow a small percentage to fail
            if (chiSquare >= criticalValue) {
                failedPositions++;
                failureDetails.append(String.format("Position %d: chi-square=%f\n", position, chiSquare));
            }
        }
        
        // Allow up to 5% of positions to fail the test (statistical variance)
        int maxAllowedFailures = (int) Math.ceil(interval * 0.05);
        assertThat(failedPositions).isLessThanOrEqualTo(maxAllowedFailures)
            .withFailMessage("Too many positions (%d) failed the chi-square test (max allowed: %d)\n%s", 
                failedPositions, maxAllowedFailures, failureDetails.toString());
    }
    
    /**
     * Test for the absence of runs (consecutive increasing or decreasing sequences)
     * in the shuffled output. A good shuffle should have minimal runs.
     */
    @Test
    void testRunsDistribution() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("runs_test.ivec");
        int interval = 1000; // Larger interval for better statistical power
        int samples = 20; // Increased sample size
        
        // Statistical analysis of runs
        List<Integer> ascendingRunsList = new ArrayList<>();
        List<Integer> descendingRunsList = new ArrayList<>();
        
        // Act - Collect data from multiple samples
        for (int sample = 0; sample < samples; sample++) {
            new CommandLine(new IvecShuffle()).execute(
                outputPath.toString(), "--interval", String.valueOf(interval), 
                "--seed", String.valueOf(sample * 100), "--force");
            
            List<Integer> values = readIvecFile(outputPath);
            
            // Count ascending and descending runs
            int ascendingRuns = countRuns(values, true);
            int descendingRuns = countRuns(values, false);
            
            ascendingRunsList.add(ascendingRuns);
            descendingRunsList.add(descendingRuns);
        }
        
        // Calculate expected runs - for a random permutation of n distinct elements,
        // the expected number of ascending runs is (n + 1) / 2
        double expectedRuns = (interval + 1) / 2.0;
        
        // Standard deviation of runs for a random permutation is sqrt((n-1)/12)
        double stdDevRuns = Math.sqrt((interval - 1) / 12.0);
        
        // Allow a wider range based on statistical analysis
        // Using 3 standard deviations for 99.7% confidence
        double zScore = 3.0; 
        double allowedDeviation = zScore * stdDevRuns;
        
        // Calculate bounds for acceptable runs
        double minExpected = expectedRuns - allowedDeviation;
        double maxExpected = expectedRuns + allowedDeviation;
        
        // Assert - Check that the average runs are within expected bounds
        double avgAscendingRuns = ascendingRunsList.stream().mapToInt(Integer::intValue).average().orElse(0);
        double avgDescendingRuns = descendingRunsList.stream().mapToInt(Integer::intValue).average().orElse(0);
        
        assertThat(avgAscendingRuns).isBetween(minExpected, maxExpected)
            .withFailMessage("Average number of ascending runs (" + avgAscendingRuns + 
                ") is outside the expected range [" + minExpected + ", " + maxExpected + "]");
        
        assertThat(avgDescendingRuns).isBetween(minExpected, maxExpected)
            .withFailMessage("Average number of descending runs (" + avgDescendingRuns + 
                ") is outside the expected range [" + minExpected + ", " + maxExpected + "]");
    }
    
    /**
     * Test that the mean and standard deviation of the shuffled sequence match
     * the expected values for the interval [0, n-1].
     */
    @Test
    void testMeanAndStandardDeviation() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("stats_test.ivec");
        int interval = 500; // Larger interval for better statistical significance
        int samples = 10; // Multiple samples for more robust testing
        
        // The expected mean for numbers 0 to n-1 is (n-1)/2
        double expectedMean = (interval - 1) / 2.0;
        
        // The expected variance for uniform distribution of integers 0 to n-1 
        // is (n^2 - 1)/12
        double expectedVariance = (Math.pow(interval, 2) - 1) / 12.0;
        double expectedStdDev = Math.sqrt(expectedVariance);
        
        // For exact shuffles, the tolerance can be very small
        // Just enough to account for floating-point precision issues
        double tolerance = 0.01; // 1% tolerance
        
        // Collect statistics across multiple samples
        double sumMean = 0.0;
        double sumStdDev = 0.0;
        
        // Act
        for (int sample = 0; sample < samples; sample++) {
            new CommandLine(new IvecShuffle()).execute(
                outputPath.toString(), "--interval", String.valueOf(interval), 
                "--seed", String.valueOf(sample * 1000), "--force");
            
            List<Integer> values = readIvecFile(outputPath);
            
            // Calculate actual mean
            double actualMean = values.stream()
                .mapToInt(Integer::intValue)
                .average()
                .orElse(0);
            
            // Calculate actual standard deviation
            double sumSquaredDiff = values.stream()
                .mapToDouble(value -> Math.pow(value - actualMean, 2))
                .sum();
            double actualVariance = sumSquaredDiff / values.size();
            double actualStdDev = Math.sqrt(actualVariance);
            
            sumMean += actualMean;
            sumStdDev += actualStdDev;
        }
        
        // Calculate averages
        double avgMean = sumMean / samples;
        double avgStdDev = sumStdDev / samples;
        
        // Assert
        assertThat(avgMean).isCloseTo(expectedMean, within(expectedMean * tolerance))
            .withFailMessage("Average mean (" + avgMean + ") is not within tolerance of expected value (" + expectedMean + ")");
        
        assertThat(avgStdDev).isCloseTo(expectedStdDev, within(expectedStdDev * tolerance))
            .withFailMessage("Average standard deviation (" + avgStdDev + ") is not within tolerance of expected value (" + expectedStdDev + ")");
    }
    
    /**
     * Test that adjacent elements in a shuffled sequence are not correlated.
     * This checks for a common flaw in poor shuffling algorithms.
     */
    @Test
    void testAdjacentElementCorrelation() throws IOException {
        // Arrange
        Path outputPath = tempDir.resolve("correlation_test.ivec");
        int interval = 5000; // Much larger interval for better correlation statistics
        int samples = 10; // More samples
        
        // Collect correlation values
        List<Double> correlations = new ArrayList<>();
        
        // Act - Generate multiple samples
        for (int sample = 0; sample < samples; sample++) {
            new CommandLine(new IvecShuffle()).execute(
                outputPath.toString(), "--interval", String.valueOf(interval), 
                "--seed", String.valueOf(sample * 1000), "--force");
            
            List<Integer> values = readIvecFile(outputPath);
            
            // Calculate correlation between adjacent elements
            double correlation = calculateAdjacentCorrelation(values);
            correlations.add(correlation);
        }
        
        // Calculate average absolute correlation
        double avgAbsCorrelation = correlations.stream()
            .mapToDouble(Math::abs)
            .average()
            .orElse(0.0);
        
        // For a good shuffle with large sample size, correlation should be very close to 0
        // The threshold should depend on the interval size
        // For n=5000, correlations of ~0.03 are reasonable
        double threshold = 0.03;
        
        // Assert
        assertThat(avgAbsCorrelation).isLessThan(threshold)
            .withFailMessage("Average absolute correlation (" + avgAbsCorrelation + 
                ") exceeds threshold " + threshold);
    }
    
    /**
     * Helper method to count runs in a sequence.
     * For statistical purposes in random permutations, a run is defined as a sequence of
     * consecutive ascending or descending elements. In this definition, a new run starts
     * whenever the direction changes.
     * 
     * @param values The sequence to analyze
     * @param ascending Whether to count ascending (true) or descending (false) runs
     * @return Number of runs found
     */
    private int countRuns(List<Integer> values, boolean ascending) {
        if (values.size() <= 1) {
            return values.size(); // 0 or 1 element means 0 or 1 run
        }
        
        int runs = 1; // Start with 1 run (first element is always part of a run)
        
        // For each adjacent pair, check if they form an ascending/descending pair
        for (int i = 1; i < values.size(); i++) {
            boolean isDesiredDirection = ascending ? 
                values.get(i) > values.get(i - 1) : 
                values.get(i) < values.get(i - 1);
                
            // Each time the pattern matches, we count it as part of a run
            if (isDesiredDirection) {
                runs++;
            }
        }
        
        return runs;
    }
    
    /**
     * Calculate Pearson correlation coefficient between adjacent elements
     */
    private double calculateAdjacentCorrelation(List<Integer> values) {
        if (values.size() <= 1) {
            return 0.0;
        }
        
        List<Integer> x = values.subList(0, values.size() - 1);
        List<Integer> y = values.subList(1, values.size());
        
        double xMean = x.stream().mapToInt(Integer::intValue).average().orElse(0);
        double yMean = y.stream().mapToInt(Integer::intValue).average().orElse(0);
        
        double sumXY = 0;
        double sumXSquared = 0;
        double sumYSquared = 0;
        
        for (int i = 0; i < x.size(); i++) {
            double xDiff = x.get(i) - xMean;
            double yDiff = y.get(i) - yMean;
            
            sumXY += xDiff * yDiff;
            sumXSquared += xDiff * xDiff;
            sumYSquared += yDiff * yDiff;
        }
        
        if (sumXSquared == 0 || sumYSquared == 0) {
            return 0.0;
        }
        
        return sumXY / Math.sqrt(sumXSquared * sumYSquared);
    }
    
    /**
     * Helper for assertThat().isCloseTo() method
     */
    private org.assertj.core.data.Offset<Double> within(double tolerance) {
        return org.assertj.core.data.Offset.offset(tolerance);
    }
    
    /**
     * Helper method to read ivec file content
     */
    private List<Integer> readIvecFile(Path path) throws IOException {
        List<Integer> values = new ArrayList<>();
        
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(path))) {
            while (dis.available() > 0) {
                int dimension = dis.readInt();
                assertTrue(dimension == 1, "Each vector should have dimension of 1");
                int value = dis.readInt();
                values.add(value);
            }
        }
        
        return values;
    }
}
