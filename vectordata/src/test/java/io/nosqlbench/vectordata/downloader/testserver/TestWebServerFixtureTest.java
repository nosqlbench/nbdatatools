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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the TestWebServerFixture class.
///
/// This test verifies that the web server fixture correctly serves catalog and dataset files,
/// and that the catalog and datasets can be accessed using the existing API.
public class TestWebServerFixtureTest {

    private TestWebServerFixture server;
    private URL baseUrl;

    @BeforeEach
    public void setUp() throws IOException {
        // Start the web server
        server = new TestWebServerFixture();
        server.start();
        baseUrl = server.getBaseUrl();

        // Update the catalog.json file with the correct server URL
        // This is necessary because the catalog.json file contains URLs with localhost:0
        // which need to be replaced with the actual server port
        String catalogJson = new String(java.nio.file.Files.readAllBytes(
            java.nio.file.Paths.get("src/test/resources/testserver/catalog.json")));
        catalogJson = catalogJson.replace("localhost:0", "localhost:" + baseUrl.getPort());
        java.nio.file.Files.write(
            java.nio.file.Paths.get("src/test/resources/testserver/catalog.json"), 
            catalogJson.getBytes());
    }

    @AfterEach
    public void tearDown() {
        // Stop the web server
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testServerStartsAndServesFiles() throws IOException {
        // Test that the server is running and serving files
        URL catalogUrl = new URL(baseUrl, "catalog.json");
        try (java.io.InputStream in = catalogUrl.openStream()) {
            assertNotNull(in);
            byte[] data = in.readAllBytes();
            assertTrue(data.length > 0);
            String content = new String(data);
            assertTrue(content.contains("testxvec"));
        }
    }

    @Test
    public void testCatalogAccess() {
        // Create a TestDataSources instance with the server URL
        TestDataSources sources = TestDataSources.ofUrl(baseUrl.toString());

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
        // Create a TestDataSources instance with the server URL
        TestDataSources sources = TestDataSources.ofUrl(baseUrl.toString());

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
        // Create a TestDataSources instance with the server URL
        TestDataSources sources = TestDataSources.ofUrl(baseUrl.toString());

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
}
