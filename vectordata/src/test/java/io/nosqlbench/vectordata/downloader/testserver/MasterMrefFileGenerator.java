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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Master generator for canonical .mref files used by other tests.
 * This test creates the single source of truth .mref files in the temp directory
 * that other tests depend on. It must run first to establish the test data foundation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MasterMrefFileGenerator {
    
    private static final Path SOURCE_DATA_DIR = 
        Paths.get("src/test/resources/testserver/rawdatasets/testxvec");
    
    /**
     * Gets the master .mref directory, which is now dynamically located under
     * the JettyFileServerExtension's temp resources root (usually target/test-classes/testserver/temp)
     */
    private static Path getMasterMrefDir() {
        return JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("generated_mref_files");
    }
    
    @Test
    @Order(1)
    public void generateCanonicalMrefFiles() throws Exception {
        System.out.println("Generating canonical .mref files for test infrastructure...");
        
        // Ensure temp directory exists
        Path tempMrefDir = getMasterMrefDir();
        Files.createDirectories(tempMrefDir);
        System.out.println("Created temp directory: " + tempMrefDir.toAbsolutePath());
        
        // Generate .mref files from static source data
        String[] sourceFiles = {
            "testxvec_base.fvec",
            "testxvec_query.fvec", 
            "testxvec_distances.fvec",
            "testxvec_indices.ivec"
        };
        
        for (String filename : sourceFiles) {
            Path sourceFile = SOURCE_DATA_DIR.resolve(filename);
            Path mrefFile = tempMrefDir.resolve(filename + ".mref");
            
            System.out.println("Processing: " + filename);
            
            // Verify source file exists
            if (!Files.exists(sourceFile)) {
                throw new RuntimeException("Source file not found: " + sourceFile.toAbsolutePath());
            }
            
            System.out.println("  Source file size: " + Files.size(sourceFile) + " bytes");
            
            // Remove existing .mref file if present
            if (Files.exists(mrefFile)) {
                Files.delete(mrefFile);
                System.out.println("  Removed existing .mref file");
            }
            
            // Generate new .mref file
            System.out.println("  Generating .mref file...");
            var progress = MerkleRefFactory.fromData(sourceFile);
            var merkleRef = progress.getFuture().get();
            
            System.out.println("  Merkle content size: " + merkleRef.getShape().getTotalContentSize());
            System.out.println("  Merkle chunk size: " + merkleRef.getShape().getChunkSize());
            
            merkleRef.save(mrefFile);
            merkleRef.close();
            
            // Verify generation succeeded
            assertTrue(Files.exists(mrefFile), "Generated .mref file should exist: " + mrefFile);
            assertTrue(Files.size(mrefFile) > 0, "Generated .mref file should not be empty: " + mrefFile);
            
            System.out.println("  Generated .mref file: " + mrefFile.getFileName() + " (" + Files.size(mrefFile) + " bytes)");
        }
        
        System.out.println("Successfully generated all canonical .mref files!");
        System.out.println("Master .mref files location: " + tempMrefDir.toAbsolutePath());
    }
}