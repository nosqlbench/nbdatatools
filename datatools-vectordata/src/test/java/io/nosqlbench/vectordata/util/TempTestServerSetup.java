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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Utility class for setting up temporary testserver directory structures
 * that include both static source files and generated .mref files.
 */
public class TempTestServerSetup {
    
    private static final Path SOURCE_RESOURCES_DIR = 
        Paths.get("src/test/resources/testserver");
    
    // Lock for synchronizing master .mref file generation
    private static final ReentrantLock MREF_GENERATION_LOCK = new ReentrantLock();
    
    // Flag to track if master .mref files have been generated
    private static volatile boolean masterMrefFilesGenerated = false;
    
    /**
     * Gets the master .mref directory, which is now dynamically located under
     * the JettyFileServerExtension's temp resources root (usually target/test-classes/testserver/temp)
     */
    private static Path getMasterMrefDir() {
        return io.nosqlbench.jetty.testserver.JettyFileServerExtension.TEMP_RESOURCES_ROOT
                .resolve("generated_mref_files");
    }
    
    /**
     * Sets up a complete temporary testserver directory structure including:
     * - Static source files (.fvec, .ivec)
     * - Catalog metadata files
     * - Master-generated .mref files
     * 
     * @param tempTestServerDir The temporary directory to set up as testserver root
     * @throws IOException if file operations fail
     */
    public static void setupTempTestServerFiles(Path tempTestServerDir) throws IOException {
        System.out.println("Setting up temp testserver structure at: " + tempTestServerDir);
        
        // Copy static catalog files
        copyCatalogFiles(tempTestServerDir);
        
        // Copy static test data files
        copyStaticTestData(tempTestServerDir);
        
        // Copy master-generated .mref files
        copyMasterMrefFiles(tempTestServerDir);
        
        System.out.println("Temp testserver setup complete");
    }
    
    /**
     * Copies catalog metadata files (catalog.json, catalog.yaml) to temp directory
     */
    private static void copyCatalogFiles(Path tempTestServerDir) throws IOException {
        System.out.println("  Copying catalog files...");
        
        // Root catalog files
        copyFile(SOURCE_RESOURCES_DIR.resolve("catalog.json"), 
                tempTestServerDir.resolve("catalog.json"));
        copyFile(SOURCE_RESOURCES_DIR.resolve("catalog.yaml"), 
                tempTestServerDir.resolve("catalog.yaml"));
        
        // Rawdatasets catalog files
        Path rawdatasetsDir = tempTestServerDir.resolve("rawdatasets");
        Files.createDirectories(rawdatasetsDir);
        
        copyFile(SOURCE_RESOURCES_DIR.resolve("rawdatasets/catalog.json"), 
                rawdatasetsDir.resolve("catalog.json"));
        copyFile(SOURCE_RESOURCES_DIR.resolve("rawdatasets/catalog.yaml"), 
                rawdatasetsDir.resolve("catalog.yaml"));
        
        System.out.println("    Copied catalog files");
    }
    
    /**
     * Copies static test data files (.fvec, .ivec, dataset metadata) to temp directory
     */
    private static void copyStaticTestData(Path tempTestServerDir) throws IOException {
        System.out.println("  Copying static test data files...");
        
        Path sourceTestxvecDir = SOURCE_RESOURCES_DIR.resolve("rawdatasets/testxvec");
        Path targetTestxvecDir = tempTestServerDir.resolve("rawdatasets/testxvec");
        Files.createDirectories(targetTestxvecDir);
        
        // Copy all static files (not .mref files)
        String[] staticFiles = {
            "README.md",
            "catalog.json", 
            "catalog.yaml",
            "dataset.yaml",
            "testxvec_base.fvec",
            "testxvec_query.fvec", 
            "testxvec_distances.fvec",
            "testxvec_indices.ivec"
        };
        
        for (String filename : staticFiles) {
            Path sourceFile = sourceTestxvecDir.resolve(filename);
            Path targetFile = targetTestxvecDir.resolve(filename);
            
            if (Files.exists(sourceFile)) {
                copyFile(sourceFile, targetFile);
                System.out.println("    Copied: " + filename + " (" + Files.size(targetFile) + " bytes)");
            } else {
                System.out.println("    Skipped missing file: " + filename);
            }
        }
    }
    
    /**
     * Copies master-generated .mref files to temp directory
     */
    private static void copyMasterMrefFiles(Path tempTestServerDir) throws IOException {
        System.out.println("  Copying master-generated .mref files...");
        
        Path targetTestxvecDir = tempTestServerDir.resolve("rawdatasets/testxvec");
        Files.createDirectories(targetTestxvecDir);
        
        String[] mrefFiles = {
            "testxvec_base.fvec.mref",
            "testxvec_query.fvec.mref",
            "testxvec_distances.fvec.mref", 
            "testxvec_indices.ivec.mref"
        };
        
        for (String filename : mrefFiles) {
            Path sourceFile = getMasterMrefDir().resolve(filename);
            Path targetFile = targetTestxvecDir.resolve(filename);
            
            if (Files.exists(sourceFile)) {
                copyFile(sourceFile, targetFile);
                System.out.println("    Copied: " + filename + " (" + Files.size(targetFile) + " bytes)");
            } else {
                System.out.println("    Warning: Master .mref file not found: " + filename);
                System.out.println("             Run MasterMrefFileGenerator first");
            }
        }
    }
    
    /**
     * Helper method to copy a file with error handling
     */
    private static void copyFile(Path source, Path target) throws IOException {
        if (!Files.exists(source)) {
            throw new IOException("Source file does not exist: " + source.toAbsolutePath());
        }
        
        // Create parent directories if they don't exist
        Files.createDirectories(target.getParent());
        
        // Copy the file
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
    
    /**
     * Ensures that master .mref files exist, generating them if necessary.
     * This method is thread-safe and will only generate the files once.
     * All callers will block until generation is complete if needed.
     * 
     * @return true if all master .mref files exist or were successfully generated
     */
    public static boolean masterMrefFilesExist() {
        // Fast path - if already generated, just verify they still exist
        if (masterMrefFilesGenerated) {
            return verifyMrefFilesExist();
        }
        
        // Acquire lock for generation check/execution
        MREF_GENERATION_LOCK.lock();
        try {
            // Double-check after acquiring lock (another thread may have generated them)
            if (masterMrefFilesGenerated) {
                return verifyMrefFilesExist();
            }
            
            // Check if files already exist
            if (verifyMrefFilesExist()) {
                masterMrefFilesGenerated = true;
                return true;
            }
            
            // Files don't exist - generate them
            System.out.println("Master .mref files not found - generating them now...");
            boolean success = generateMasterMrefFiles();
            
            if (success) {
                masterMrefFilesGenerated = true;
                System.out.println("Master .mref files generated successfully");
            } else {
                System.err.println("Failed to generate master .mref files");
            }
            
            return success;
            
        } finally {
            MREF_GENERATION_LOCK.unlock();
        }
    }
    
    /**
     * Verifies that all required .mref files exist.
     * 
     * @return true if all files exist, false otherwise
     */
    private static boolean verifyMrefFilesExist() {
        String[] mrefFiles = {
            "testxvec_base.fvec.mref",
            "testxvec_query.fvec.mref",
            "testxvec_distances.fvec.mref", 
            "testxvec_indices.ivec.mref"
        };
        
        for (String filename : mrefFiles) {
            Path mrefFile = getMasterMrefDir().resolve(filename);
            if (!Files.exists(mrefFile)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Generates the master .mref files from source data.
     * This is equivalent to what MasterMrefFileGenerator does, but callable directly.
     * 
     * @return true if generation succeeded, false otherwise
     */
    private static boolean generateMasterMrefFiles() {
        try {
            Path sourceDataDir = Paths.get("src/test/resources/testserver/rawdatasets/testxvec");
            Path tempMrefDir = getMasterMrefDir();
            
            // Ensure directory exists
            Files.createDirectories(tempMrefDir);
            System.out.println("  Master .mref directory: " + tempMrefDir.toAbsolutePath());
            
            // Generate .mref files from static source data
            String[] sourceFiles = {
                "testxvec_base.fvec",
                "testxvec_query.fvec", 
                "testxvec_distances.fvec",
                "testxvec_indices.ivec"
            };
            
            for (String filename : sourceFiles) {
                Path sourceFile = sourceDataDir.resolve(filename);
                Path mrefFile = tempMrefDir.resolve(filename + ".mref");
                
                System.out.println("  Generating: " + filename + ".mref");
                
                // Verify source file exists
                if (!Files.exists(sourceFile)) {
                    System.err.println("    ERROR: Source file not found: " + sourceFile.toAbsolutePath());
                    return false;
                }
                
                // Remove existing .mref file if present
                if (Files.exists(mrefFile)) {
                    Files.delete(mrefFile);
                }
                
                // Generate new .mref file
                var progress = MerkleRefFactory.fromData(sourceFile);
                var merkleRef = progress.getFuture().get();
                
                merkleRef.save(mrefFile);
                merkleRef.close();
                
                // Verify generation succeeded
                if (!Files.exists(mrefFile)) {
                    System.err.println("    ERROR: Failed to generate .mref file: " + mrefFile);
                    return false;
                }
                
                System.out.println("    Generated: " + mrefFile.getFileName() + " (" + Files.size(mrefFile) + " bytes)");
            }
            
            return true;
            
        } catch (Exception e) {
            System.err.println("Error generating master .mref files: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}