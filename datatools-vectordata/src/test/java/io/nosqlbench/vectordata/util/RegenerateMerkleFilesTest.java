package io.nosqlbench.vectordata.util;

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


import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Test to regenerate .mref files for test data with correct merklev2 format.
 * This test uses isolated temp directories to avoid conflicts with other tests.
 */
public class RegenerateMerkleFilesTest {
    
    private static final Path SOURCE_DATA_DIR = 
        Paths.get("src/test/resources/testserver/rawdatasets/testxvec");
    
    @Test
    public void testRegenerationInIsolation(@TempDir Path tempDir) throws Exception {
        System.out.println("Testing .mref regeneration in isolated temp directory: " + tempDir);
        
        String[] testFiles = {
            "testxvec_base.fvec",
            "testxvec_distances.fvec", 
            "testxvec_indices.ivec",
            "testxvec_query.fvec"
        };
        
        // Copy source files to temp directory for isolated testing
        copySourceFilesToTempDir(tempDir, testFiles);
        
        // Test regeneration in isolation
        for (String filename : testFiles) {
            Path dataPath = tempDir.resolve(filename);
            Path mrefPath = tempDir.resolve(filename + ".mref");
            
            System.out.println("Regenerating: " + filename);
            
            // Ensure no existing .mref file
            Files.deleteIfExists(mrefPath);
            
            // Create new merkle data from source file using merklev2
            var progress = MerkleRefFactory.fromData(dataPath);
            var merkleData = progress.getFuture().get();
            
            // Save the merkle data
            merkleData.save(mrefPath);
            
            System.out.println("Generated: " + mrefPath.getFileName() + " (size: " + Files.size(mrefPath) + " bytes)");
            
            // Verify the regenerated file can be loaded
            var loaded = MerkleRefFactory.load(mrefPath);
            System.out.println("  Verified loadable - content size: " + loaded.getShape().getTotalContentSize());
            loaded.close();
            
            // Close the original merkle data
            merkleData.close();
        }
        
        System.out.println("Successfully tested .mref regeneration in isolation!");
    }
    
    private void copySourceFilesToTempDir(Path tempDir, String[] filenames) throws IOException {
        System.out.println("Copying source files to temp directory for isolated testing...");
        
        for (String filename : filenames) {
            Path sourceFile = SOURCE_DATA_DIR.resolve(filename);
            Path targetFile = tempDir.resolve(filename);
            
            if (!Files.exists(sourceFile)) {
                throw new RuntimeException("Source file not found: " + sourceFile.toAbsolutePath());
            }
            
            Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("  Copied: " + filename + " (" + Files.size(targetFile) + " bytes)");
        }
    }
}
