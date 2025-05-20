package io.nosqlbench.command.catalog_hdf5;

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


import io.nosqlbench.command.catalog_hdf5.CMD_catalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ValidCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void testValidCatalog() throws IOException {
        // Create a simple directory structure
        Path testDir = tempDir.resolve("test_dir");
        Files.createDirectories(testDir);
        
        // Create a valid dataset.yaml file with proper format based on actual test files
        Path datasetYaml = testDir.resolve("dataset.yaml");
        String yaml = "attributes:\n" +
                      "  model: test_model1\n" +
                      "  url: https://example.com/test1\n" +
                      "  distance_function: COSINE\n" +
                      "  license: MIT\n" +
                      "  vendor: Test Vendor\n" +
                      "  notes: This is a test dataset\n" +
                      "  tags:\n" +
                      "    tag1: value1\n" +
                      "    tag2: value2\n" +
                      "profiles:\n" +
                      "  default:\n" +
                      "    base:\n" +
                      "      source: test.hdf5\n" +
                      "      window: 1000\n" +
                      "    indices:\n" +
                      "      source: indices.bin\n" +
                      "    distances:\n" +
                      "      source: distances.bin\n";
        Files.writeString(datasetYaml, yaml);
        
        // Create a dummy HDF5 file
        Path hdf5File = testDir.resolve("test.hdf5");
        byte[] data = new byte[1024];
        Files.write(hdf5File, data);
        
        // Create dummy indices and distances files
        Path indicesFile = testDir.resolve("indices.bin");
        Path distancesFile = testDir.resolve("distances.bin");
        Files.write(indicesFile, new byte[512]);
        Files.write(distancesFile, new byte[512]);
        
        // Print debug info
        System.out.println("Test directory structure created at: " + tempDir.toAbsolutePath());
        System.out.println("testDir: " + testDir.toAbsolutePath());
        System.out.println("datasetYaml: " + datasetYaml.toAbsolutePath());
        System.out.println("hdf5File: " + hdf5File.toAbsolutePath());
        
        // Execute the catalog command
        CMD_catalog cmd = new CMD_catalog();
        CommandLine commandLine = new CommandLine(cmd);
        
        System.out.println("Executing catalog command with path: " + testDir.toAbsolutePath());
        int exitCode = commandLine.execute(testDir.toString());
        System.out.println("Exit code: " + exitCode);
        
        // Check if catalog files were created
        Path catalogJson = testDir.resolve("catalog.json");
        Path catalogYaml = testDir.resolve("catalog.yaml");
        
        System.out.println("Catalog JSON exists: " + Files.exists(catalogJson));
        System.out.println("Catalog YAML exists: " + Files.exists(catalogYaml));
        
        if (Files.exists(catalogJson)) {
            System.out.println("Catalog JSON content: " + Files.readString(catalogJson));
        }
    }
}
