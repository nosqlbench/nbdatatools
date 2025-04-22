package io.nosqlbench.nbvectors.commands.catalog_hdf5;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SimpleCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void testSimpleCatalog() throws IOException {
        // Create a simple directory structure
        Path testDir = tempDir.resolve("test_dir");
        Files.createDirectories(testDir);
        
        // Create a simple dataset.yaml file
        Path datasetYaml = testDir.resolve("dataset.yaml");
        String yaml = "---\n" +
                      "name: Test Dataset\n" +
                      "description: A test dataset\n" +
                      "version: 1.0\n" +
                      "profiles:\n" +
                      "  default:\n" +
                      "    dimensions: 128\n" +
                      "attributes:\n" +
                      "  model: test-model\n" +
                      "  url: http://example.com\n" +
                      "  distance_function: COSINE\n" +
                      "  license: Apache-2.0\n" +
                      "  vendor: nosqlbench\n";
        Files.writeString(datasetYaml, yaml);
        
        // Create a dummy HDF5 file
        Path hdf5File = testDir.resolve("test.hdf5");
        byte[] data = new byte[1024];
        Files.write(hdf5File, data);
        
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
