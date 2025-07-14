package io.nosqlbench.vectordata.downloader;

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
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Test class for verifying access to Cohere dataset through the test data framework.
/// This test uses the built-in Jetty webserver fixture to serve test data and
/// verifies that vector data can be properly accessed and validated.
@ExtendWith(JettyFileServerExtension.class)
@Disabled
public class CohereAccessTest {

    private TestDataSources sources;

    /// Sets up the test environment by configuring the test data sources.
    /// Uses the built-in webserver fixture for accessing test data.
    /// 
    /// @throws IOException if there's an error setting up the test environment
    @BeforeEach
    public void setUp() throws IOException {
        // Use the built-in webserver fixture for access
        // The Catalog class will append "catalog.json" to this URL
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Create a TestDataSources instance with the webserver URL to the directory
        String realdataUrl = baseUrl.toString() + "temp/realdata";
        sources = TestDataSources.ofUrl(realdataUrl);
    }

    /// Tests access to the Cohere dataset through the catalog system.
    /// This test verifies that:
    /// - The Cohere dataset can be found in the catalog
    /// - Vector data can be accessed and has correct dimensions
    /// - Vectors contain valid non-zero float values
    /// - Random sampling of vectors works correctly
    /// 
    /// The test will fail if the Cohere dataset is not available or accessible.
    @Test
    @Disabled
    public void testDatasetAccess() {
        Catalog catalog = sources.catalog();
        List<DatasetEntry> datasets = catalog.datasets();

        // Print all available datasets
        System.out.println("Available datasets:");
        datasets.forEach(System.out::println);

        // Find the cohere dataset
        Optional<DatasetEntry> datasetOpt = catalog.matchOne("cohere");
        assertTrue(datasetOpt.isPresent(), "Cohere dataset should be present in the catalog");

        DatasetEntry dataset = datasetOpt.get();
        System.out.println("Found dataset: " + dataset.name());
        System.out.println("Attributes: " + dataset.attributes());

        // Select the 1m profile (smaller dataset for testing)
        ProfileSelector profiles = dataset.select();
        TestDataView dataView = profiles.profile("10m");

        // Get base vectors
        BaseVectors baseVectors = dataView.getBaseVectors()
            .orElseThrow(() -> new RuntimeException("Base vectors not found"));

        int count = baseVectors.getCount();
        System.out.println("Vector count: " + count);
        int dimensions = baseVectors.getVectorDimensions();
        System.out.println("Vector dimensions: " + dimensions);

        // Verify we have vectors to test
        assertTrue(count > 0, "Dataset should contain vectors, but count is: " + count);

        // Verify vectors by randomly sampling across the vector space
        System.out.println("Verifying vectors with random sampling...");
        Random random = new Random();
        int maxSamples = 1000;

        baseVectors.get(count-1);
        for (int sample = 0; sample < maxSamples; sample++) {
            // Generate a random index within the prebuffered range
            int randomIndex = random.nextInt(count);

            float[] vector = baseVectors.get(randomIndex);
            assertNotNull(vector, "Vector at index " + randomIndex + " should not be null");
            assertEquals(dimensions, vector.length, "Vector at index " + randomIndex + " should have correct dimensions");

            // Verify that the vector contains valid float values (not NaN or Infinity)
            for (int j = 0; j < dimensions; j++) {
                assertTrue(!Float.isNaN(vector[j]) && !Float.isInfinite(vector[j]), 
                          "Vector at index " + randomIndex + ", dimension " + j + " should be a valid float value");
            }

            // Verify that the vector contains non-zero values
            boolean hasNonZero = false;
            for (int j = 0; j < dimensions; j++) {
                if (vector[j] != 0.0f) {
                    hasNonZero = true;
                    break;
                }
            }
            assertTrue(hasNonZero, "Vector at index " + randomIndex + " should contain non-zero values");

            // Print a sample of the vector values (only for the first 10 samples to avoid excessive output)
            if (sample < 10) {
                System.out.println("Sample " + sample + " (vector " + randomIndex + "): " + formatVectorSample(vector, 5));
            }
        }

        System.out.println("Vector verification completed successfully");
    }

    /// Helper method to format a sample of vector values for display.
    /// Only shows the first few values of the vector followed by "..." if truncated.
    /// 
    /// @param vector The vector to format
    /// @param sampleSize The maximum number of values to display
    /// @return A formatted string representation of the vector sample
    private String formatVectorSample(float[] vector, int sampleSize) {
        StringBuilder sb = new StringBuilder("[");
        int size = Math.min(sampleSize, vector.length);

        for (int i = 0; i < size; i++) {
            sb.append(vector[i]);
            if (i < size - 1) {
                sb.append(", ");
            }
        }

        if (size < vector.length) {
            sb.append(", ...");
        }

        sb.append("]");
        return sb.toString();
    }
}
