package io.nosqlbench.nbvectors.commands.catalog_hdf5;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FixedCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void testFixedCatalog() throws IOException {
        // Create a simple directory structure
        Path testDir = tempDir.resolve("test_dir");
        Files.createDirectories(testDir);
        
        // Create a valid dataset.yaml file with proper format
        Path datasetYaml = testDir.resolve("dataset.yaml");
        String yaml = "---\n" +
                      "name: Test Dataset\n" +
                      "description: A test dataset\n" +
                      "version: 1.0\n" +
                      "profiles:\n" +
                      "  default:\n" +
                      "    base_vectors:\n" +
                      "      dimensions: 128\n" +
                      "      count: 1000\n" +
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
