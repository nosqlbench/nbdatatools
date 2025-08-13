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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Utility class for setting up temporary testserver directory structures
 * that include both static source files and generated .mref files.
 */
public class TempTestServerSetup {
    
    private static final Path SOURCE_RESOURCES_DIR = 
        Paths.get("src/test/resources/testserver");
    private static final Path MASTER_MREF_DIR = 
        Paths.get("src/test/resources/testserver/temp/generated_mref_files");
    
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
            Path sourceFile = MASTER_MREF_DIR.resolve(filename);
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
     * Ensures that master .mref files exist before setting up temp testserver.
     * This helps provide clear error messages if dependencies aren't met.
     * 
     * @return true if all master .mref files exist, false otherwise
     */
    public static boolean masterMrefFilesExist() {
        String[] mrefFiles = {
            "testxvec_base.fvec.mref",
            "testxvec_query.fvec.mref",
            "testxvec_distances.fvec.mref", 
            "testxvec_indices.ivec.mref"
        };
        
        for (String filename : mrefFiles) {
            Path mrefFile = MASTER_MREF_DIR.resolve(filename);
            if (!Files.exists(mrefFile)) {
                System.out.println("Missing master .mref file: " + mrefFile);
                return false;
            }
        }
        
        return true;
    }
}