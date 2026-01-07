package io.nosqlbench.vshapes.extract;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link DimensionFitReport}.
 */
@Tag("unit")
public class DimensionFitReportTest {

    @Test
    void testComputeWithMixedDistributions() {
        // Generate data with mixed distributions
        Random random = new Random(42);
        int numVectors = 500;
        int dimensions = 5;
        float[][] data = new float[numVectors][dimensions];

        for (int v = 0; v < numVectors; v++) {
            // Dim 0: Normal distribution
            data[v][0] = (float) (random.nextGaussian() * 0.2 + 0.5);

            // Dim 1: Uniform distribution
            data[v][1] = random.nextFloat();

            // Dim 2: Beta-like distribution (skewed)
            data[v][2] = (float) Math.pow(random.nextFloat(), 2);

            // Dim 3: Another normal
            data[v][3] = (float) (random.nextGaussian() * 0.3 + 0.3);

            // Dim 4: Another uniform
            data[v][4] = random.nextFloat() * 0.5f + 0.25f;
        }

        // Transpose data for DimensionFitReport
        float[][] transposed = transpose(data, numVectors, dimensions);

        DimensionFitReport report = DimensionFitReport.compute(transposed);

        // Verify basic properties
        assertEquals(dimensions, report.numDimensions());
        assertFalse(report.modelTypes().isEmpty());

        // Verify each dimension has a best fit
        for (int d = 0; d < dimensions; d++) {
            assertNotNull(report.getBestFit(d));
            assertFalse(report.getBestFit(d).isEmpty());
        }

        // Test formatting
        String table = report.formatTable();
        assertNotNull(table);
        assertTrue(table.contains("Dim"));
        assertTrue(table.contains("Best"));
        assertTrue(table.contains("Summary"));

        System.out.println("=== Full Fit Report ===");
        System.out.println(table);
    }

    @Test
    void testFormatTableWithMaxDimensions() {
        Random random = new Random(42);
        int numVectors = 200;
        int dimensions = 20;
        float[][] data = new float[numVectors][dimensions];

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = random.nextFloat();
            }
        }

        float[][] transposed = transpose(data, numVectors, dimensions);
        DimensionFitReport report = DimensionFitReport.compute(transposed);

        // Format with limit
        String limitedTable = report.formatTable(5);
        assertTrue(limitedTable.contains("15 more dimensions"));

        System.out.println("=== Limited Table (5 dims) ===");
        System.out.println(limitedTable);
    }

    @Test
    void testSummary() {
        Random random = new Random(42);
        int numVectors = 100;
        int dimensions = 10;
        float[][] data = new float[numVectors][dimensions];

        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                data[v][d] = random.nextFloat();
            }
        }

        float[][] transposed = transpose(data, numVectors, dimensions);
        DimensionFitReport report = DimensionFitReport.compute(transposed);

        String summary = report.formatSummary();
        assertNotNull(summary);
        assertTrue(summary.contains("Best Fit Distribution Summary"));
        assertTrue(summary.contains("dimensions"));

        System.out.println("=== Summary Only ===");
        System.out.println(summary);
    }

    private float[][] transpose(float[][] data, int rows, int cols) {
        float[][] transposed = new float[cols][rows];
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                transposed[c][r] = data[r][c];
            }
        }
        return transposed;
    }
}
