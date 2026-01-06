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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for sparkline generation during model extraction.
 */
public class SparklineIntegrationTest {

    @Test
    void testSparklineGenerationInExtraction() {
        // Create test data - 1000 vectors, 10 dimensions
        Random rng = new Random(42);
        float[][] data = new float[1000][10];

        for (int v = 0; v < 1000; v++) {
            for (int d = 0; d < 10; d++) {
                if (d < 5) {
                    // Normal distribution for first 5 dimensions
                    data[v][d] = (float) rng.nextGaussian();
                } else {
                    // Uniform distribution for last 5 dimensions
                    data[v][d] = rng.nextFloat();
                }
            }
        }

        // Extract with all fits collection (which includes sparklines)
        DatasetModelExtractor extractor = new DatasetModelExtractor()
            .withAllFitsCollection();

        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

        System.out.println("Extraction complete!");
        assertTrue(result.hasAllFitsData(), "Should have all fits data");

        ModelExtractor.AllFitsData allFits = result.allFitsData();
        assertTrue(allFits.hasSparklines(), "Should have sparklines");
        assertEquals(10, allFits.numDimensions(), "Should have 10 dimensions");

        System.out.println("\nSparklines per dimension:");
        for (int d = 0; d < allFits.numDimensions(); d++) {
            String sparkline = allFits.getSparkline(d);
            assertNotNull(sparkline, "Sparkline should not be null for dimension " + d);
            assertEquals(Sparkline.DEFAULT_WIDTH, sparkline.length(),
                "Sparkline should have default width for dimension " + d);
            System.out.printf("  Dim %2d: %s  (best: %s)%n",
                d, sparkline, allFits.getBestModelType(d));
        }

        // Create fit report and verify it includes sparklines
        DimensionFitReport report = DimensionFitReport.fromAllFitsData(allFits);
        String table = report.formatTable();

        System.out.println("\nFit Table with Sparklines:");
        System.out.println(table);

        // Verify the table has sparklines
        assertTrue(table.contains("Distribution"), "Table should have Distribution column header");

        // Verify alignment
        String[] lines = table.split("\n");
        int headerLen = -1;
        for (String line : lines) {
            if (line.isEmpty() || line.startsWith("...") || line.startsWith("Best Fit")) {
                continue;
            }
            // Skip summary lines
            if (line.matches("\\s+\\w+.*:.*dimensions.*")) {
                continue;
            }
            if (headerLen < 0) {
                headerLen = line.length();
            } else if (line.startsWith("Dim") || line.startsWith("---") || line.matches("\\s*\\d+.*")) {
                assertEquals(headerLen, line.length(),
                    "Line should have consistent length: '" + line + "'");
            }
        }
    }

    @Test
    void testVirtualThreadExtractorSparklines() {
        // Create test data
        Random rng = new Random(12345);
        float[][] data = new float[500][8];

        for (int v = 0; v < 500; v++) {
            for (int d = 0; d < 8; d++) {
                data[v][d] = (float) (rng.nextGaussian() * 0.5 + d);
            }
        }

        // Use VirtualThreadModelExtractor with all fits collection
        VirtualThreadModelExtractor extractor = VirtualThreadModelExtractor.builder()
            .selector(BestFitSelector.defaultSelector())
            .uniqueVectors(1000)
            .collectAllFits()
            .build();

        ModelExtractor.ExtractionResult result = extractor.extractWithStats(data);

        assertTrue(result.hasAllFitsData(), "Should have all fits data");
        ModelExtractor.AllFitsData allFits = result.allFitsData();
        assertTrue(allFits.hasSparklines(), "Should have sparklines");

        System.out.println("\nVirtual Thread Extractor - Sparklines:");
        for (int d = 0; d < allFits.numDimensions(); d++) {
            System.out.printf("  Dim %d: %s%n", d, allFits.getSparkline(d));
        }
    }
}
