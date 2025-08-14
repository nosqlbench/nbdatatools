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

package io.nosqlbench.command.generate;

import io.nosqlbench.command.generate.subcommands.CMD_generate_ivecShuffle;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/// Statistical test class for the IvecShuffle command.
/// This class focuses on validating the statistical properties
/// of the shuffle operation, such as uniformity of distribution.
public class IvecShuffleStatisticalTest {

    @TempDir
    Path tempDir;

    /// Test that the distribution of values is uniform using a chi-square test.
    /// This verifies that all numbers appear with roughly equal frequency over multiple runs.
    /// 
    /// Note: Statistical tests are inherently probabilistic, and some variance is expected.
    /// We allow a reasonable percentage of positions to fail the chi-square test.
    @Test
    void testUniformDistribution() throws Exception {
        // Arrange
        Path outputPath = tempDir.resolve("uniform_test.ivec");
        int interval = 100;
        int runs = 200; // Multiple runs for statistical significance

        // Maps to count occurrences of each value in different positions
        Map<Integer, Map<Integer, Integer>> positionCounts = new HashMap<>();

        // Initialize the position counts map
        for (int i = 0; i < interval; i++) {
            positionCounts.put(i, new HashMap<>());
            for (int j = 0; j < interval; j++) {
                positionCounts.get(i).put(j, 0);
            }
        }

        // Act - Run multiple shuffles and count occurrences
        for (int i = 0; i < runs; i++) {
            // Create a fresh shuffle for each run
            CMD_generate_ivecShuffle command = new CMD_generate_ivecShuffle();
            CommandLine cmd = new CommandLine(command);
            int exitCode = cmd.execute(
                "--interval", String.valueOf(interval),
                "--output", outputPath.toString(),
                "--force"
            );

            // Read the shuffled file and count occurrences
            try (VectorFileArray<int[]> reader =
                     VectorFileIO.randomAccess(FileType.xvec,int[].class, outputPath)) {
                for (int pos = 0; pos < interval; pos++) {
                    int[] vector = reader.get(pos);
                    int value = vector[0]; // Each vector has only one value

                    // Increment the count for this value at this position
                    Map<Integer, Integer> countsAtPosition = positionCounts.get(pos);
                    countsAtPosition.put(value, countsAtPosition.get(value) + 1);
                }
            }
        }

        // Assert - Perform Chi-Square goodness of fit test
        ChiSquareTest chiSquareTest = new ChiSquareTest();
        int failedPositions = 0;

        for (int pos = 0; pos < interval; pos++) {
            // Create expected and observed frequency arrays
            double[] expected = new double[interval];
            long[] observed = new long[interval];

            // Expected frequency should be uniform
            double expectedFrequency = (double) runs / interval;
            for (int i = 0; i < interval; i++) {
                expected[i] = expectedFrequency;
                observed[i] = positionCounts.get(pos).get(i);
            }

            // Perform chi-square test
            double pValue = chiSquareTest.chiSquareTest(expected, observed);

            // Allow a small percentage of positions to fail
            // For chi-square test, p-value < 0.05 typically indicates rejection of uniformity
            if (pValue < 0.01) {
                failedPositions++;
            }
        }

        // Assert that most positions pass the chi-square test
        // Allow up to 5% of positions to fail due to natural statistical variation
        double failureRate = (double) failedPositions / interval;
        assert failureRate <= 0.05 : 
            String.format("Too many positions failed the chi-square test: %d out of %d (%.2f%%)",
                failedPositions, interval, failureRate * 100);
    }
}
