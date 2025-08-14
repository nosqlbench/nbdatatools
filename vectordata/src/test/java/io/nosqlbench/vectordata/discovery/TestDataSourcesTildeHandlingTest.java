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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for tilde (~) handling in TestDataSources and Catalog
public class TestDataSourcesTildeHandlingTest {

    @TempDir
    Path tempDir;

    private String originalHome;

    @BeforeEach
    public void setUp() {
        // Save the original home property if it exists
        originalHome = System.getProperty("test.home.override");
        // Set the test home directory to our temp directory
        System.setProperty("test.home.override", tempDir.toString());
    }

    @AfterEach
    public void tearDown() {
        // Restore the original property
        if (originalHome != null) {
            System.setProperty("test.home.override", originalHome);
        } else {
            System.clearProperty("test.home.override");
        }
    }

    /// Test that tilde in string catalog paths is expanded correctly
    @Test
    public void testTildeInStringCatalogPath() throws IOException {
        // Create a test catalog in our mock home directory
        Path testCatalogDir = tempDir.resolve("test-catalog-tilde");
        Files.createDirectories(testCatalogDir);
        Path catalogFile = testCatalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "tilde-test-dataset",
              "url": "http://example.com/dataset.hdf5",
              "attributes": {"test": "tilde-expansion"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Use tilde notation to reference the catalog
        TestDataSources sources = new TestDataSources()
            .addCatalogs("~/test-catalog-tilde");

        // Create catalog - should resolve tilde correctly
        Catalog catalog = sources.catalog();

        // Verify the catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("tilde-test-dataset", datasets.get(0).name());
        assertEquals("tilde-expansion", datasets.get(0).attributes().get("test"));
    }

    /// Test that tilde in optional catalog paths is expanded correctly
    @Test
    public void testTildeInOptionalCatalogPath() throws IOException {
        // Create a test catalog in our mock home directory
        Path testCatalogDir = tempDir.resolve("test-optional-tilde");
        Files.createDirectories(testCatalogDir);
        Path catalogFile = testCatalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "optional-tilde-dataset",
              "url": "http://example.com/optional.hdf5",
              "attributes": {"type": "optional-tilde"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Use tilde notation for optional catalog
        TestDataSources sources = new TestDataSources()
            .addOptionalCatalogs("~/test-optional-tilde");

        // Create catalog - should resolve tilde correctly
        Catalog catalog = sources.catalog();

        // Verify the optional catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("optional-tilde-dataset", datasets.get(0).name());
        assertEquals("optional-tilde", datasets.get(0).attributes().get("type"));
    }

    /// Test that tilde in config directory paths is expanded correctly
    @Test
    public void testTildeInConfigDirectory() throws IOException {
        // Create a test config directory in our mock home
        Path testConfigDir = tempDir.resolve("test-config-tilde");
        Path catalogDir = tempDir.resolve("catalog");
        
        // Create config directory with catalogs.yaml
        Files.createDirectories(testConfigDir);
        Files.createDirectories(catalogDir);
        
        // Create catalog file
        Path catalogFile = catalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "config-tilde-dataset",
              "url": "http://example.com/config.hdf5",
              "attributes": {"source": "config-tilde"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create catalogs.yaml in config directory
        Path catalogsYaml = testConfigDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, 
            "- " + catalogDir.toString() + "\n");

        // Use tilde notation for config directory
        TestDataSources sources = new TestDataSources()
            .configure(Path.of("~/test-config-tilde"));

        // Create catalog - should resolve tilde correctly
        Catalog catalog = sources.catalog();

        // Verify the catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("config-tilde-dataset", datasets.get(0).name());
        assertEquals("config-tilde", datasets.get(0).attributes().get("source"));
    }

    /// Test that tilde in default configuration paths works
    @Test
    public void testTildeInDefaultConfiguration() throws IOException {
        // Create the default config structure in our mock home
        Path configDir = tempDir.resolve(".config").resolve("nbvectors");
        Files.createDirectories(configDir);
        
        // Create an empty catalogs.yaml to ensure predictable behavior
        Path catalogsYaml = configDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, "[]");
        
        TestDataSources sources = new TestDataSources();
        
        // These should not throw and should use our mock home directory
        TestDataSources withOptional = assertDoesNotThrow(() -> 
            sources.configureOptional()
        );
        
        // Verify that we can still add catalogs after using default config
        Path catalogDir = tempDir.resolve("test-catalog");
        Files.createDirectories(catalogDir);
        Path catalogFile = catalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "test-dataset",
              "url": "http://example.com/test.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);
        
        TestDataSources finalSources = withOptional.addCatalogs(catalogDir.toString());
        Catalog catalog = finalSources.catalog();
        
        // Should only have the one catalog we added, not any from the optional config
        assertEquals(1, catalog.datasets().size());
        assertEquals("test-dataset", catalog.datasets().get(0).name());
    }

    /// Test that multiple tildes in paths are handled correctly
    @Test
    public void testMultipleTildesInPath() throws IOException {
        // Create nested directories in our mock home
        Path nestedDir = tempDir.resolve("test1").resolve("test2");
        Files.createDirectories(nestedDir);
        Path catalogFile = nestedDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "nested-dataset",
              "url": "http://example.com/nested.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Use tilde notation
        TestDataSources sources = new TestDataSources()
            .addCatalogs("~/test1/test2");

        Catalog catalog = sources.catalog();
        assertEquals(1, catalog.datasets().size());
        assertEquals("nested-dataset", catalog.datasets().get(0).name());
    }

    /// Test that paths without tilde are not affected
    @Test
    public void testPathsWithoutTilde() throws IOException {
        // Create catalog without using tilde
        Path catalogDir = tempDir.resolve("no-tilde-catalog");
        Files.createDirectories(catalogDir);
        Path catalogFile = catalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "no-tilde-dataset",
              "url": "http://example.com/notilde.hdf5",
              "attributes": {"tilde": "none"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Use absolute path without tilde
        TestDataSources sources = new TestDataSources()
            .addCatalogs(catalogDir.toString());

        Catalog catalog = sources.catalog();
        assertEquals(1, catalog.datasets().size());
        assertEquals("no-tilde-dataset", catalog.datasets().get(0).name());
        assertEquals("none", catalog.datasets().get(0).attributes().get("tilde"));
    }

    /// Test that missing optional catalogs with tilde don't cause errors
    @Test
    public void testMissingOptionalCatalogWithTilde() throws IOException {
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

        // Reference a non-existent optional catalog using tilde
        TestDataSources sources = new TestDataSources()
            .addCatalogs(requiredDir.toString())
            .addOptionalCatalogs("~/non-existent-catalog");

        // Create catalog - should not throw despite missing optional catalog
        Catalog catalog = assertDoesNotThrow(() -> sources.catalog());

        // Verify only the required catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("required-dataset", datasets.get(0).name());
    }

    /// Test that tilde works in YAML catalog lists
    @Test
    public void testTildeInYamlCatalogList() throws IOException {
        // Create config directory
        Path configDir = tempDir.resolve("config");
        Files.createDirectories(configDir);
        
        // Create a catalog directory in mock home
        Path homeCatalogDir = tempDir.resolve("my-catalogs");
        Files.createDirectories(homeCatalogDir);
        Path catalogFile = homeCatalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "yaml-tilde-dataset",
              "url": "http://example.com/yaml.hdf5",
              "attributes": {"from": "yaml"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create catalogs.yaml with tilde path
        Path catalogsYaml = configDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, "- ~/my-catalogs\n");

        // Configure using the config directory
        TestDataSources sources = new TestDataSources()
            .configure(configDir);

        // Create catalog - should resolve tilde in YAML
        Catalog catalog = sources.catalog();

        // Verify the catalog was loaded
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("yaml-tilde-dataset", datasets.get(0).name());
        assertEquals("yaml", datasets.get(0).attributes().get("from"));
    }

    /// Test the specific issue where tilde paths were being appended to CWD
    @Test
    public void testTildePathNotAppendedToCwd() throws IOException {
        // Create the .config/testapp structure in mock home
        Path configPath = tempDir.resolve(".config").resolve("testapp");
        Files.createDirectories(configPath);
        
        // Create a catalog file
        Path catalogFile = configPath.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "testapp-dataset",
              "url": "http://example.com/testapp.hdf5",
              "attributes": {"path": "correct"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // This should expand to <tempDir>/.config/testapp, NOT <cwd>/~/.config/testapp
        TestDataSources sources = new TestDataSources()
            .addCatalogs("~/.config/testapp");

        Catalog catalog = sources.catalog();
        
        // Verify the catalog was loaded from the correct location
        List<DatasetEntry> datasets = catalog.datasets();
        assertEquals(1, datasets.size());
        assertEquals("testapp-dataset", datasets.get(0).name());
        assertEquals("correct", datasets.get(0).attributes().get("path"));
    }

    /// Test that deeply nested tilde paths work correctly
    @Test
    public void testDeeplyNestedTildePath() throws IOException {
        // Create a deeply nested structure
        Path deepPath = tempDir.resolve(".config").resolve("myapp").resolve("catalogs.yaml");
        Files.createDirectories(deepPath.getParent());
        
        // Create catalog directory
        Path catalogDir = tempDir.resolve("app-catalogs");
        Files.createDirectories(catalogDir);
        Path catalogFile = catalogDir.resolve("catalog.json");
        Files.writeString(catalogFile, """
            [{
              "name": "deep-dataset",
              "url": "http://example.com/deep.hdf5",
              "attributes": {},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create catalogs.yaml pointing to catalog
        Files.writeString(deepPath, "- " + catalogDir.toString() + "\n");

        // Use tilde path to config directory
        TestDataSources sources = new TestDataSources()
            .configure(Path.of("~/.config/myapp"));

        Catalog catalog = sources.catalog();
        assertEquals(1, catalog.datasets().size());
        assertEquals("deep-dataset", catalog.datasets().get(0).name());
    }

    /// Test that directory paths automatically look for catalogs.yaml
    @Test
    public void testDirectoryPathFindssCatalogsYaml() throws IOException {
        // Create a directory with catalogs.yaml
        Path catalogDir = tempDir.resolve("catalog-directory");
        Files.createDirectories(catalogDir);
        
        // Create data catalog directory
        Path dataCatalogDir = tempDir.resolve("data-catalog");
        Files.createDirectories(dataCatalogDir);
        Path dataFile = dataCatalogDir.resolve("catalog.json");
        Files.writeString(dataFile, """
            [{
              "name": "directory-test-dataset",
              "url": "http://example.com/directory.hdf5",
              "attributes": {"source": "directory"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create catalogs.yaml in the directory
        Path catalogsYaml = catalogDir.resolve("catalogs.yaml");
        Files.writeString(catalogsYaml, "- " + dataCatalogDir.toString() + "\n");

        // Use directory path directly (should find catalogs.yaml automatically)
        TestDataSources sources = new TestDataSources()
            .addCatalogs(catalogDir.toString());

        Catalog catalog = sources.catalog();
        assertEquals(1, catalog.datasets().size());
        assertEquals("directory-test-dataset", catalog.datasets().get(0).name());
        assertEquals("directory", catalog.datasets().get(0).attributes().get("source"));
    }

    /// Test that YAML file paths are handled directly
    @Test
    public void testYamlFilePathHandledDirectly() throws IOException {
        // Create data catalog directory
        Path dataCatalogDir = tempDir.resolve("yaml-catalog");
        Files.createDirectories(dataCatalogDir);
        Path dataFile = dataCatalogDir.resolve("catalog.json");
        Files.writeString(dataFile, """
            [{
              "name": "yaml-file-dataset",
              "url": "http://example.com/yamlfile.hdf5",
              "attributes": {"source": "yaml-file"},
              "profiles": {},
              "tags": {}
            }]
            """);

        // Create a YAML file directly
        Path yamlFile = tempDir.resolve("my-catalogs.yaml");
        Files.writeString(yamlFile, "- " + dataCatalogDir.toString() + "\n");

        // Use YAML file path directly
        TestDataSources sources = new TestDataSources()
            .addCatalogs(yamlFile.toString());

        Catalog catalog = sources.catalog();
        assertEquals(1, catalog.datasets().size());
        assertEquals("yaml-file-dataset", catalog.datasets().get(0).name());
        assertEquals("yaml-file", catalog.datasets().get(0).attributes().get("source"));
    }

    /// Test that directory without catalogs.yaml throws error
    @Test
    public void testDirectoryWithoutCatalogsYamlThrowsError() throws IOException {
        // Create a directory without catalogs.yaml
        Path emptyDir = tempDir.resolve("empty-directory");
        Files.createDirectories(emptyDir);

        // Should throw exception when directory doesn't contain catalogs.yaml
        TestDataSources sources = new TestDataSources();
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            sources.addCatalogs(emptyDir.toString());
        });
        
        assertTrue(exception.getMessage().contains("does not contain catalogs.yaml or catalog.json"));
        assertTrue(exception.getMessage().contains(emptyDir.toString()));
    }
}