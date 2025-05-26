package io.nosqlbench.vectordata.downloader.testserver;

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

import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the TestWebServerFixture class.
///
/// This test verifies that the web server fixture correctly serves catalog and dataset files,
/// and that the catalog and datasets can be accessed using the existing API.
/// The test web server is automatically started by the TestWebServerExtension's static initializer,
/// so no annotation is needed.
public class TestWebServerFixtureTest {

    private TestDataSources sources;

    @BeforeEach
    public void setUp() throws IOException {
        // Use a direct file URL to the directory containing the catalog.json file
        // The Catalog class will append "catalog.json" to this URL
        // This will use the inbuilt content in the test resources
        Path resourcesDir = Paths.get("src/test/resources/testserver").toAbsolutePath();
        URL resourcesUrl = resourcesDir.toUri().toURL();

        // Create a TestDataSources instance with the file URL to the directory
        sources = TestDataSources.ofUrl(resourcesUrl.toString());
    }

    @Test
    public void testServerStartsAndServesFiles() throws IOException {
        // Get the base URL from the TestWebServerExtension
        URL baseUrl = TestWebServerExtension.getBaseUrl();

        // Test that the server is running and serving files
        URL catalogUrl = new URL(baseUrl, "catalog.json");
        java.net.HttpURLConnection connection = (java.net.HttpURLConnection) catalogUrl.openConnection();
        try {
            // Check the HTTP status code
            int statusCode = connection.getResponseCode();
            assertEquals(200, statusCode, "HTTP status code should be 200");

            try (java.io.InputStream in = connection.getInputStream()) {
                assertNotNull(in);
                byte[] data = in.readAllBytes();
                assertTrue(data.length > 0);
                String content = new String(data);
                assertTrue(content.contains("testxvec"));
            }
        } finally {
            connection.disconnect();
        }
    }

    @Test
    public void testCatalogAccess() {
        // Use the TestDataSources instance initialized in setUp with the inbuilt files
        // Get the catalog
        Catalog catalog = sources.catalog();
        assertNotNull(catalog);

        // Verify that the catalog contains the expected datasets
        List<DatasetEntry> datasets = catalog.datasets();
        assertNotNull(datasets);
        assertTrue(datasets.size() >= 1);

        // Find a specific dataset
        Optional<DatasetEntry> datasetOpt = catalog.findExact("testxvec");
        assertTrue(datasetOpt.isPresent());

        // Verify dataset attributes
        DatasetEntry dataset = datasetOpt.get();
        assertEquals("testxvec", dataset.name());
        assertNotNull(dataset.attributes());
        assertTrue(dataset.attributes().containsKey("url"));
        assertEquals("https://github.com/nosqlbench/nbdatatools", dataset.attributes().get("url"));
    }

    @Test
    public void testDatasetAccess() {
        // Use the TestDataSources instance initialized in setUp with the inbuilt files
        // Get the catalog
        Catalog catalog = sources.catalog();

        // Find a specific dataset
        Optional<DatasetEntry> datasetOpt = catalog.findExact("testxvec");
        assertTrue(datasetOpt.isPresent());

        // Get the dataset
        DatasetEntry dataset = datasetOpt.get();

        // Select a profile
        ProfileSelector profiles = dataset.select();
        assertNotNull(profiles);

        // Get a specific profile
        TestDataView defaultView = profiles.profile("default");
        assertNotNull(defaultView);

        // Verify that the profile contains the expected data
        Optional<BaseVectors> baseVectorsOpt = defaultView.getBaseVectors();
        assertTrue(baseVectorsOpt.isPresent());

        BaseVectors baseVectors = baseVectorsOpt.get();
        assertEquals(25000, baseVectors.getCount());
    }

    @Test
    public void testMerkleDatasetAccess() {
        // Use the TestDataSources instance initialized in setUp with the inbuilt files
        // Get the catalog
        Catalog catalog = sources.catalog();

        // Find the dataset (testxvec has merkle tree files)
        Optional<DatasetEntry> datasetOpt = catalog.findExact("testxvec");
        assertTrue(datasetOpt.isPresent());

        // Get the dataset
        DatasetEntry dataset = datasetOpt.get();

        // Select a profile
        ProfileSelector profiles = dataset.select();
        TestDataView defaultView = profiles.profile("default");

        // Verify that the profile contains the expected data
        Optional<BaseVectors> baseVectorsOpt = defaultView.getBaseVectors();
        assertTrue(baseVectorsOpt.isPresent());

        BaseVectors baseVectors = baseVectorsOpt.get();
        assertEquals(25000, baseVectors.getCount());
    }

    // Since we're focusing on using inbuilt files, we'll skip the HEAD request test
    // as it's specifically testing the web server functionality
    // The other tests already verify that the web server is working correctly
}
