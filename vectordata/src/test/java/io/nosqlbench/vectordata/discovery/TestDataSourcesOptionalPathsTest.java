package io.nosqlbench.vectordata.discovery;

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

import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for TestDataSources and Catalog interaction with optional paths
public class TestDataSourcesOptionalPathsTest {

    @TempDir
    Path tempDir;

    /// Test that optional catalogs that exist are loaded successfully
    @Test
    public void testOptionalCatalogsExistingAreLoaded() throws IOException {
        // Create a test catalog file
        Path catalogFile = tempDir.resolve("catalog.json");
        String catalogContent = """
            [
              {
                "name": "test-dataset-1",
                "url": "http://example.com/dataset1.hdf5",
                "attributes": {
                  "description": "Test dataset 1"
                },
                "profiles": {},
                "tags": {
                  "type": "test"
                }
              },
              {
                "name": "test-dataset-2",
                "url": "http://example.com/dataset2.hdf5",
                "attributes": {
                  "description": "Test dataset 2"
                },
                "profiles": {},
                "tags": {
                  "type": "test"
                }
              }
            ]
            """;
        Files.writeString(catalogFile, catalogContent);

        // Create TestDataSources with the catalog as optional
        TestDataSources sources = new TestDataSources()
            .addOptionalCatalogs(tempDir.toString());

        // Create catalog from sources
        Catalog catalog = sources.catalog();

        // Verify that the optional catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(2, datasets.size());
        
        // Verify dataset entries
        DatasetEntry dataset1 = datasets.stream()
            .filter(d -> d.name().equals("test-dataset-1"))
            .findFirst()
            .orElse(null);
        assertNotNull(dataset1);
        assertEquals("Test dataset 1", dataset1.attributes().get("description"));
        assertEquals("test", dataset1.tags().get("type"));

        DatasetEntry dataset2 = datasets.stream()
            .filter(d -> d.name().equals("test-dataset-2"))
            .findFirst()
            .orElse(null);
        assertNotNull(dataset2);
        assertEquals("Test dataset 2", dataset2.attributes().get("description"));
        assertEquals("test", dataset2.tags().get("type"));
    }

    /// Test that missing optional catalogs don't cause errors
    @Test
    public void testOptionalCatalogsMissingDontCauseErrors() throws IOException {
        // Create a required catalog that exists
        Path requiredDir = tempDir.resolve("required");
        Files.createDirectories(requiredDir);
        Path requiredCatalog = requiredDir.resolve("catalog.json");
        Files.writeString(requiredCatalog, """
            [{
              "name": "required-dataset",
              "url": "http://example.com/required.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Reference a non-existent optional catalog
        Path nonExistentDir = tempDir.resolve("non-existent");

        // Create TestDataSources with required and optional catalogs
        TestDataSources sources = new TestDataSources()
            .addCatalogs(requiredDir.toString())
            .addOptionalCatalogs(nonExistentDir.toString());

        // Create catalog - should not throw despite missing optional catalog
        Catalog catalog = assertDoesNotThrow(() -> sources.catalog());

        // Verify only the required catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("required-dataset", datasets.get(0).name());
    }

    /// Test that required catalogs throw errors when missing
    @Test
    public void testRequiredCatalogsMissingCauseErrors() {
        // Reference a non-existent required catalog
        Path nonExistentDir = tempDir.resolve("non-existent");

        // Create TestDataSources with missing required catalog
        TestDataSources sources = new TestDataSources()
            .addCatalogs(nonExistentDir.toString());

        // Attempting to create catalog should throw
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> sources.catalog());
        assertTrue(exception.getMessage().contains("Failed to load catalog"));
    }

    /// Test mixing required and optional catalogs
    @Test
    public void testMixedRequiredAndOptionalCatalogs() throws IOException {
        // Create required catalog
        Path requiredDir = tempDir.resolve("required");
        Files.createDirectories(requiredDir);
        Path requiredCatalog = requiredDir.resolve("catalog.json");
        Files.writeString(requiredCatalog, """
            [{
              "name": "required-dataset",
              "url": "http://example.com/required.hdf5",
              "attributes": {"source": "required"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create optional catalog that exists
        Path optionalDir1 = tempDir.resolve("optional1");
        Files.createDirectories(optionalDir1);
        Path optionalCatalog1 = optionalDir1.resolve("catalog.json");
        Files.writeString(optionalCatalog1, """
            [{
              "name": "optional-dataset-1",
              "url": "http://example.com/optional1.hdf5",
              "attributes": {"source": "optional1"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create another optional catalog that doesn't exist
        Path optionalDir2 = tempDir.resolve("optional2");

        // Create TestDataSources with mixed catalogs
        TestDataSources sources = new TestDataSources()
            .addCatalogs(requiredDir.toString())
            .addOptionalCatalogs(optionalDir1.toString(), optionalDir2.toString());

        // Create catalog
        Catalog catalog = sources.catalog();

        // Verify both existing catalogs were loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(2, datasets.size());

        // Verify we have the required dataset
        assertTrue(datasets.stream().anyMatch(d -> 
            d.name().equals("required-dataset") && 
            "required".equals(d.attributes().get("source"))));

        // Verify we have the optional dataset that exists
        assertTrue(datasets.stream().anyMatch(d -> 
            d.name().equals("optional-dataset-1") && 
            "optional1".equals(d.attributes().get("source"))));
    }

    /// Test that optional catalogs from configuration directories work
    @Test
    public void testOptionalCatalogsFromConfigDir() throws IOException {
        // Create config directory with catalogs.yaml
        Path configDir = tempDir.resolve("config");
        Files.createDirectories(configDir);
        
        // Create a catalog directory
        Path catalogDir = tempDir.resolve("catalog");
        Files.createDirectories(catalogDir);
        Path catalogFile = catalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "config-dataset",
              "url": "http://example.com/config.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create catalogs.yaml pointing to the catalog
        Path catalogsYaml = configDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, 
            "- " + catalogDir.toString() + "\n");

        // Create TestDataSources using configureOptional
        TestDataSources sources = new TestDataSources()
            .configureOptional(configDir);

        // Create catalog
        Catalog catalog = sources.catalog();

        // Verify the catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("config-dataset", datasets.get(0).name());
    }

    /// Test that empty catalogs.yaml for optional configuration doesn't cause errors
    @Test
    public void testEmptyOptionalConfigFile() throws IOException {
        // Create config directory with empty catalogs.yaml
        Path configDir = tempDir.resolve("config");
        Files.createDirectories(configDir);
        Path catalogsYaml = configDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, "[]");

        // Add a required catalog so we have something
        Path requiredDir = tempDir.resolve("required");
        Files.createDirectories(requiredDir);
        Path requiredCatalog = requiredDir.resolve("catalog.json");
        Files.writeString(requiredCatalog, """
            [{
              "name": "test-dataset",
              "url": "http://example.com/test.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create TestDataSources with empty optional config
        TestDataSources sources = new TestDataSources()
            .addCatalogs(requiredDir.toString())
            .configureOptional(configDir);

        // Create catalog - should work despite empty optional config
        Catalog catalog = assertDoesNotThrow(() -> sources.catalog());
        
        // Should only have the required catalog entry
        assertEquals(1, catalog.datasets().size());
        assertEquals("test-dataset", catalog.datasets().get(0).name());
    }
}