package io.nosqlbench.vectordata.merklev2;

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
import io.nosqlbench.vectordata.util.TestFixturePaths;
import io.nosqlbench.vectordata.util.TempTestServerSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the MAFileChannel constructor initialization logic according to the three cases
 * specified in the class documentation:
 * 1. No cache file and no mrkl state file - downloads mref and creates both
 * 2. Both cache file and mrkl state file exist - loads both as-is
 * 3. Any other state combination - throws error
 * 
 * Now uses isolated temp testserver structures to avoid conflicts with other tests.
 */
@ExtendWith(JettyFileServerExtension.class)  
class MAFileChannelCreateTest {

    private String testServerUrl;
    
    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        // Check that master .mref files exist before proceeding
        assumeTrue(TempTestServerSetup.masterMrefFilesExist(), 
            "Requires master .mref files - run MasterMrefFileGenerator first");
        
        // Create test-specific directory in temp testserver area
        String testName = "MAFC_" + testInfo.getTestMethod().get().getName() + "_" + System.currentTimeMillis();
        Path tempTestServerDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve(testName);
        
        // Set up complete temp testserver structure with .mref files
        TempTestServerSetup.setupTempTestServerFiles(tempTestServerDir);
        
        // Create base URL for this test's temp server directory
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        testServerUrl = baseUrl.toString() + "temp/" + testName + "/";
        
        System.out.println("MAFileChannelCreateTest setup - serving from: " + testServerUrl);
    }

    @Test
    void testCase1_NoFilesExist_DownloadsAndCreates(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Case 1: No cache file and no mrkl state file exist
        // Should download remote mref file, create mrkl state file from it, discard mref
        
        // Use isolated temp testserver URL
        URL remoteUrl = new URL(testServerUrl + "rawdatasets/testxvec/testxvec_base.fvec");
        
        // Use test-specific filenames to avoid conflicts
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "state_file.mrkl");
        
        Path localCache = tempDir.resolve(cacheFilename);
        Path stateFile = tempDir.resolve(stateFilename);
        
        // Verify preconditions - neither file exists
        assertFalse(Files.exists(localCache), "Cache file should not exist initially");
        assertFalse(Files.exists(stateFile), "State file should not exist initially");
        
        // Create MAFileChannel - should trigger Case 1 logic
        try (MAFileChannel channel = new MAFileChannel(localCache, stateFile, remoteUrl.toString())) {
            assertNotNull(channel);
            
            // Verify both files were created
            assertTrue(Files.exists(localCache), "Cache file should be created");
            assertTrue(Files.exists(stateFile), "State file should be created");
            
            // Verify the channel works
            assertTrue(channel.size() > 0, "Channel should report positive size");
        }
    }
    
    @Test  
    void testCase2_BothFilesExist_LoadsAsIs(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Case 2: Both cache file and mrkl state file exist
        // Should load both as-is with no modification
        
        // Use isolated temp testserver URL
        URL remoteUrl = new URL(testServerUrl + "rawdatasets/testxvec/testxvec_query.fvec");
        
        // Use test-specific filenames to avoid conflicts
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "existing_cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "existing_state.mrkl");
        
        Path localCache = tempDir.resolve(cacheFilename);
        Path mrklFile = tempDir.resolve(stateFilename);
        
        // First, create both files using Case 1 logic (fresh initialization)
        try (MAFileChannel setupChannel = new MAFileChannel(localCache, mrklFile, remoteUrl.toString())) {
            // Just create the files, then close
        }
        
        // Verify both files now exist
        assertTrue(Files.exists(localCache), "Cache file should exist after setup");
        assertTrue(Files.exists(mrklFile), "State file should exist after setup");
        
        // Record original file sizes and timestamps
        long originalCacheSize = Files.size(localCache);
        long originalMrklSize = Files.size(mrklFile);
        long originalCacheTime = Files.getLastModifiedTime(localCache).toMillis();
        long originalMrklTime = Files.getLastModifiedTime(mrklFile).toMillis();
        
        // Small delay to ensure any file modifications would show different timestamps
        Thread.sleep(10);
        
        // Create MAFileChannel with existing files - should trigger Case 2 logic
        try (MAFileChannel channel = new MAFileChannel(localCache, mrklFile, remoteUrl.toString())) {
            assertNotNull(channel);
            
            // Verify files were loaded as-is (no modification to size or timestamp)
            assertEquals(originalCacheSize, Files.size(localCache), "Cache file should not be modified");
            assertEquals(originalMrklSize, Files.size(mrklFile), "State file should not be modified");
            assertEquals(originalCacheTime, Files.getLastModifiedTime(localCache).toMillis(), "Cache file timestamp should not change");
            assertEquals(originalMrklTime, Files.getLastModifiedTime(mrklFile).toMillis(), "State file timestamp should not change");
            
            // Verify the channel works
            assertTrue(channel.size() > 0, "Channel should report positive size");
        }
    }
    
    @Test
    void testCase3_OnlyCacheExists_ThrowsError(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Case 3a: Cache file exists but mrkl state file does not
        // Should throw an error
        
        // Use isolated temp testserver URL
        URL remoteUrl = new URL(testServerUrl + "rawdatasets/testxvec/testxvec_base.fvec");
        
        // Use test-specific filenames to avoid conflicts
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "only_cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "missing_state.mrkl");
        
        Path localCache = tempDir.resolve(cacheFilename);
        Path mrklFile = tempDir.resolve(stateFilename);
        
        // Create only the cache file
        Files.write(localCache, "dummy cache content".getBytes());
        
        // Verify preconditions
        assertTrue(Files.exists(localCache), "Cache file should exist");
        assertFalse(Files.exists(mrklFile), "State file should not exist");
        
        // Attempt to create MAFileChannel should throw IOException
        IOException exception = assertThrows(IOException.class, () -> {
            new MAFileChannel(localCache, mrklFile, remoteUrl.toString());
        });
        
        assertTrue(exception.getMessage().contains("Invalid initialization state"),
                   "Exception should mention invalid initialization state");
        assertTrue(exception.getMessage().contains("cache file"),
                   "Exception should mention cache file state");
        assertTrue(exception.getMessage().contains("mrkl state file"),
                   "Exception should mention mrkl state file state");
    }
    
    @Test
    void testCase3_OnlyStateExists_ThrowsError(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Case 3b: Mrkl state file exists but cache file does not
        // Should throw an error
        
        // Use isolated temp testserver URL
        URL remoteUrl = new URL(testServerUrl + "rawdatasets/testxvec/testxvec_query.fvec");
        
        // Use test-specific filenames to avoid conflicts
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "missing_cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "only_state.mrkl");
        
        Path localCache = tempDir.resolve(cacheFilename);
        Path mrklFile = tempDir.resolve(stateFilename);
        
        // Create a valid mrkl file first (using Case 1), then delete the cache
        try (MAFileChannel setupChannel = new MAFileChannel(localCache, mrklFile, remoteUrl.toString())) {
            // Create both files
        }
        
        // Delete only the cache file to create invalid state
        Files.delete(localCache);
        
        // Verify preconditions
        assertFalse(Files.exists(localCache), "Cache file should not exist");
        assertTrue(Files.exists(mrklFile), "State file should exist");
        
        // Attempt to create MAFileChannel should throw IOException
        IOException exception = assertThrows(IOException.class, () -> {
            new MAFileChannel(localCache, mrklFile, remoteUrl.toString());
        });
        
        assertTrue(exception.getMessage().contains("Invalid initialization state"),
                   "Exception should mention invalid initialization state");
        assertTrue(exception.getMessage().contains("Either both files must exist or neither must exist"),
                   "Exception should explain the valid state combinations");
    }
    
    @Test
    void testMrklExtensionEnforcement(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        // Test that .mrkl extension is automatically added when missing
        
        // Use isolated temp testserver URL
        URL remoteUrl = new URL(testServerUrl + "rawdatasets/testxvec/testxvec_base.fvec");
        
        // Use test-specific filenames to avoid conflicts
        String cacheFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "cache.dat");
        String stateFilename = TestFixturePaths.createTestSpecificFilename(testInfo, "state_file");
        
        Path localCache = tempDir.resolve(cacheFilename);
        Path statePathWithoutExtension = tempDir.resolve(stateFilename);  // No .mrkl extension
        
        // Create MAFileChannel
        try (MAFileChannel channel = new MAFileChannel(localCache, statePathWithoutExtension, remoteUrl.toString())) {
            assertNotNull(channel);
            
            // Verify that a .mrkl file was created, not the original path
            Path expectedMrklFile = statePathWithoutExtension.resolveSibling(statePathWithoutExtension.getFileName() + ".mrkl");
            assertTrue(Files.exists(expectedMrklFile), "Expected .mrkl file should exist: " + expectedMrklFile);
            
            // Verify the original path without extension was NOT created as a state file
            assertFalse(Files.exists(statePathWithoutExtension), "Original path without .mrkl extension should not exist as state file");
        }
    }
    
}