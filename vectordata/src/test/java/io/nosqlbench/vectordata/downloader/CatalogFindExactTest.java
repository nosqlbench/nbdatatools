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

import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for enhanced error messaging in Catalog.findExact method
public class CatalogFindExactTest {

    private PrintStream originalErr;
    private ByteArrayOutputStream capturedErr;

    @BeforeEach
    public void setUp() {
        // Capture stderr output
        originalErr = System.err;
        capturedErr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(capturedErr));
    }

    @AfterEach
    public void tearDown() {
        // Restore original stderr
        System.setErr(originalErr);
    }

    /// Test that exact match returns the dataset without any error output
    @Test
    public void testExactMatchFound() {
        // Create test datasets
        List<DatasetEntry> datasets = List.of(
            createTestDataset("mnist-784-euclidean"),
            createTestDataset("glove-100-angular"),
            createTestDataset("sift-128-euclidean")
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("mnist-784-euclidean");
        
        assertTrue(result.isPresent());
        assertEquals("mnist-784-euclidean", result.get().name());
        
        // Should not print anything to stderr for successful match
        String errorOutput = capturedErr.toString();
        assertTrue(errorOutput.isEmpty(), "Expected no error output, but got: " + errorOutput);
    }

    /// Test that no match found prints suggestions with substring matches
    @Test
    public void testNoMatchWithSubstringMatches() {
        // Create test datasets
        List<DatasetEntry> datasets = List.of(
            createTestDataset("mnist-784-euclidean"),
            createTestDataset("mnist-fashion-784"),
            createTestDataset("glove-100-angular"),
            createTestDataset("sift-128-euclidean")
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("mnist");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should contain the not found message
        assertTrue(errorOutput.contains("Dataset 'mnist' not found."));
        
        // Should contain substring suggestions with profiles
        assertTrue(errorOutput.contains("Did you mean one of these datasets?"));
        assertTrue(errorOutput.contains("- mnist-784-euclidean (profiles: default)"));
        assertTrue(errorOutput.contains("- mnist-fashion-784 (profiles: default)"));
        
        // Should contain the full list of available datasets with profiles
        assertTrue(errorOutput.contains("Available datasets (4 total):"));
        assertTrue(errorOutput.contains("- glove-100-angular (profiles: default)"));
        assertTrue(errorOutput.contains("- sift-128-euclidean (profiles: default)"));
    }

    /// Test that no match found with no substring matches still shows all datasets
    @Test
    public void testNoMatchWithoutSubstringMatches() {
        // Create test datasets
        List<DatasetEntry> datasets = List.of(
            createTestDataset("mnist-784-euclidean"),
            createTestDataset("glove-100-angular"),
            createTestDataset("sift-128-euclidean")
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("nonexistent");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should contain the not found message
        assertTrue(errorOutput.contains("Dataset 'nonexistent' not found."));
        
        // Should NOT contain substring suggestions since there are no matches
        assertFalse(errorOutput.contains("Did you mean one of these datasets?"));
        
        // Should contain the full list of available datasets with profiles
        assertTrue(errorOutput.contains("Available datasets (3 total):"));
        assertTrue(errorOutput.contains("- mnist-784-euclidean (profiles: default)"));
        assertTrue(errorOutput.contains("- glove-100-angular (profiles: default)"));
        assertTrue(errorOutput.contains("- sift-128-euclidean (profiles: default)"));
    }

    /// Test empty catalog behavior
    @Test
    public void testEmptyCatalog() {
        Catalog catalog = new Catalog(List.of());
        
        Optional<DatasetEntry> result = catalog.findExact("anything");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should contain the not found message
        assertTrue(errorOutput.contains("Dataset 'anything' not found."));
        
        // Should contain the empty catalog message
        assertTrue(errorOutput.contains("No datasets are available in the catalog."));
    }

    /// Test case insensitive substring matching
    @Test
    public void testCaseInsensitiveSubstringMatching() {
        // Create test datasets
        List<DatasetEntry> datasets = List.of(
            createTestDataset("MNIST-784-Euclidean"),
            createTestDataset("GloVe-100-Angular")
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("mnist");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should contain substring suggestions with profiles (case insensitive)
        assertTrue(errorOutput.contains("Did you mean one of these datasets?"));
        assertTrue(errorOutput.contains("- MNIST-784-Euclidean (profiles: default)"));
        
        // Should not suggest GloVe since it doesn't contain "mnist"
        assertFalse(errorOutput.contains("- GloVe-100-Angular") && 
                   !errorOutput.contains("Available datasets"));
    }

    /// Test multiple exact matches (should throw exception)
    @Test
    public void testMultipleExactMatches() {
        // Create test datasets with same name (different case)
        List<DatasetEntry> datasets = List.of(
            createTestDataset("test-dataset"),
            createTestDataset("TEST-DATASET")  // Should match case-insensitively
        );
        
        Catalog catalog = new Catalog(datasets);
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            catalog.findExact("test-dataset");
        });
        
        assertTrue(exception.getMessage().contains("Found multiple datasets matching test-dataset"));
    }

    /// Test that datasets with multiple profiles show all profile names
    @Test
    public void testMultipleProfilesDisplayed() {
        // Create test datasets with multiple profiles
        DSProfileGroup multipleProfiles = new DSProfileGroup();
        multipleProfiles.put("default", new DSProfile(Map.of()));
        multipleProfiles.put("small", new DSProfile(Map.of()));
        multipleProfiles.put("large", new DSProfile(Map.of()));
        
        List<DatasetEntry> datasets = List.of(
            createTestDatasetWithProfiles("multi-profile-dataset", multipleProfiles),
            createTestDataset("simple-dataset")
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("nonexistent");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should show multiple profiles comma-separated
        assertTrue(errorOutput.contains("- multi-profile-dataset (profiles: default, small, large)"));
        assertTrue(errorOutput.contains("- simple-dataset (profiles: default)"));
    }

    /// Test that datasets with no profiles show appropriate message
    @Test
    public void testNoProfilesDisplayed() {
        // Create test dataset with empty profiles
        DSProfileGroup emptyProfiles = new DSProfileGroup();
        
        List<DatasetEntry> datasets = List.of(
            createTestDatasetWithProfiles("no-profile-dataset", emptyProfiles)
        );
        
        Catalog catalog = new Catalog(datasets);
        
        Optional<DatasetEntry> result = catalog.findExact("nonexistent");
        
        assertFalse(result.isPresent());
        
        String errorOutput = capturedErr.toString();
        
        // Should show "no profiles" message
        assertTrue(errorOutput.contains("- no-profile-dataset (no profiles)"));
    }

    /// Helper method to create test dataset entries
    private DatasetEntry createTestDataset(String name) {
        DSProfileGroup profiles = new DSProfileGroup();
        profiles.put("default", new DSProfile(Map.of()));
        return createTestDatasetWithProfiles(name, profiles);
    }

    /// Helper method to create test dataset entries with custom profiles
    private DatasetEntry createTestDatasetWithProfiles(String name, DSProfileGroup profiles) {
        try {
            return new DatasetEntry(
                name,
                new java.net.URL("http://example.com/" + name + ".hdf5"),
                Map.of("test", "true"),
                profiles,
                Map.of()
            );
        } catch (java.net.MalformedURLException e) {
            throw new RuntimeException("Invalid test URL", e);
        }
    }
}