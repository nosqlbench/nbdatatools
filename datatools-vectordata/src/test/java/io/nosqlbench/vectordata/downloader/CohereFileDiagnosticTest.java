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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/// Diagnostic test for investigating Cohere file access issues.
/// This test performs detailed analysis of the file structure and calculations
/// to identify the source of integer overflow and dimension reading problems.
@ExtendWith(JettyFileServerExtension.class)
public class CohereFileDiagnosticTest {

    private TestDataSources sources;

    @BeforeEach
    public void setUp() throws IOException {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String realdataUrl = baseUrl.toString() + "temp/realdata";
        sources = TestDataSources.ofUrl(realdataUrl);
    }

    @Test
    public void diagnoseCohereFileStructure() {
        System.out.println("=== Cohere File Structure Diagnostic ===");
        
        try {
            // Find the cohere dataset
            Catalog catalog = sources.catalog();
            Optional<DatasetEntry> datasetOpt = catalog.matchOne("cohere");
            
            if (!datasetOpt.isPresent()) {
                System.out.println("Cohere dataset not found - skipping diagnostic");
                return;
            }
            
            DatasetEntry dataset = datasetOpt.get();
            System.out.println("Dataset found: " + dataset.name());
            System.out.println("Dataset attributes: " + dataset.attributes());
            
            // Get the profile and data view
            ProfileSelector profiles = dataset.select();
            TestDataView dataView = profiles.profile("10m");
            
            // Try to get the file path directly if possible
            // This might not work depending on the implementation, but let's try
            System.out.println("Data view: " + dataView.getClass().getSimpleName());
            
            // Try to access base vectors to trigger the error
            System.out.println("\n=== Attempting to access BaseVectors ===");
            try {
                BaseVectors baseVectors = dataView.getBaseVectors()
                    .orElseThrow(() -> new RuntimeException("Base vectors not found"));
                
                System.out.println("BaseVectors class: " + baseVectors.getClass().getSimpleName());
                
                // Get the problematic values
                int count = baseVectors.getCount();
                int dimensions = baseVectors.getVectorDimensions();
                
                System.out.println("Vector count: " + count);
                System.out.println("Vector dimensions: " + dimensions);
                
                // Check if count is negative (indicating overflow)
                if (count < 0) {
                    System.out.println("ERROR: Vector count is negative! This indicates integer overflow.");
                    System.out.println("Count as unsigned: " + Integer.toUnsignedString(count));
                    System.out.println("Count as long: " + Integer.toUnsignedLong(count));
                }
                
                // Try to access reflection to get internal file size if possible
                try {
                    java.lang.reflect.Field[] fields = baseVectors.getClass().getSuperclass().getSuperclass().getDeclaredFields();
                    for (java.lang.reflect.Field field : fields) {
                        field.setAccessible(true);
                        if (field.getName().equals("sourceSize")) {
                            long sourceSize = (Long) field.get(baseVectors);
                            System.out.println("Source file size: " + sourceSize + " bytes");
                            System.out.println("Source file size: " + (sourceSize / 1024 / 1024) + " MB");
                            System.out.println("Source file size: " + (sourceSize / 1024 / 1024 / 1024) + " GB");
                            
                            // Calculate what the record size should be
                            int componentBytes = 4; // float = 4 bytes
                            long recordSize = 4 + (dimensions * componentBytes);
                            System.out.println("Calculated record size: " + recordSize + " bytes");
                            
                            // Calculate count as long to see if it would overflow
                            long countAsLong = sourceSize / recordSize;
                            System.out.println("Count as long: " + countAsLong);
                            System.out.println("Count cast to int: " + (int)countAsLong);
                            
                            if (countAsLong > Integer.MAX_VALUE) {
                                System.out.println("ERROR: Count exceeds Integer.MAX_VALUE (" + Integer.MAX_VALUE + ")");
                                System.out.println("This is the source of the integer overflow!");
                            }
                        }
                        if (field.getName().equals("dimensions")) {
                            int dims = (Integer) field.get(baseVectors);
                            System.out.println("Internal dimensions field: " + dims);
                        }
                        if (field.getName().equals("componentBytes")) {
                            int compBytes = (Integer) field.get(baseVectors);
                            System.out.println("Component bytes: " + compBytes);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Could not access internal fields via reflection: " + e.getMessage());
                }
                
                // Try to read the first vector to see if dimensions are correct
                System.out.println("\n=== Attempting to read first vector ===");
                try {
                    float[] firstVector = baseVectors.get(0);
                    System.out.println("First vector length: " + firstVector.length);
                    System.out.println("First few values: ");
                    for (int i = 0; i < Math.min(10, firstVector.length); i++) {
                        System.out.printf("  [%d]: %.6f\n", i, firstVector[i]);
                    }
                } catch (Exception e) {
                    System.out.println("Error reading first vector: " + e.getMessage());
                    e.printStackTrace();
                }
                
            } catch (Exception e) {
                System.out.println("Error accessing BaseVectors: " + e.getMessage());
                e.printStackTrace();
            }
            
        } catch (Exception e) {
            System.out.println("Error in diagnostic: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Test
    public void analyzeFileDirectly() {
        System.out.println("\n=== Direct File Analysis ===");
        
        // Try to find the actual file path
        String[] possiblePaths = {
            "testserver/temp/realdata/cohere_wiki_en_flat_base_10m_norm.fvecs",
            "temp/realdata/cohere_wiki_en_flat_base_10m_norm.fvecs",
            "realdata/cohere_wiki_en_flat_base_10m_norm.fvecs",
            "cohere_wiki_en_flat_base_10m_norm.fvecs"
        };
        
        for (String pathStr : possiblePaths) {
            Path path = Paths.get(pathStr);
            if (Files.exists(path)) {
                System.out.println("Found file at: " + path.toAbsolutePath());
                analyzeFileStructure(path);
                return;
            }
        }
        
        System.out.println("Could not find Cohere file in any of the expected locations:");
        for (String pathStr : possiblePaths) {
            System.out.println("  " + Paths.get(pathStr).toAbsolutePath());
        }
    }
    
    private void analyzeFileStructure(Path filePath) {
        try {
            long fileSize = Files.size(filePath);
            System.out.println("File size: " + fileSize + " bytes (" + (fileSize / 1024 / 1024) + " MB)");
            
            // Read the first few bytes to check the dimension header
            byte[] header = Files.readAllBytes(filePath);
            if (header.length >= 4) {
                ByteBuffer buffer = ByteBuffer.wrap(header, 0, 4);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                int dimensions = buffer.getInt();
                
                System.out.println("Dimensions from file header: " + dimensions);
                
                if (dimensions <= 0 || dimensions > 100000) {
                    System.out.println("ERROR: Invalid dimensions value! Expected something like 768 or 1024 for Cohere embeddings.");
                    
                    // Try reading as big-endian
                    buffer.rewind();
                    buffer.order(ByteOrder.BIG_ENDIAN);
                    int dimensionsBE = buffer.getInt();
                    System.out.println("Dimensions as big-endian: " + dimensionsBE);
                    
                    // Show raw bytes
                    System.out.printf("Raw header bytes: %02x %02x %02x %02x\n", 
                        header[0], header[1], header[2], header[3]);
                }
                
                // Calculate expected record size and count
                int componentBytes = 4; // float
                long recordSize = 4 + (dimensions * componentBytes);
                System.out.println("Expected record size: " + recordSize + " bytes");
                
                if (recordSize > 0) {
                    long countAsLong = fileSize / recordSize;
                    System.out.println("Expected vector count (as long): " + countAsLong);
                    System.out.println("Expected vector count (cast to int): " + (int)countAsLong);
                    
                    if (countAsLong > Integer.MAX_VALUE) {
                        System.out.println("ERROR: Count would overflow integer! Max int value: " + Integer.MAX_VALUE);
                    }
                }
                
                // Try reading the first vector
                if (header.length >= recordSize) {
                    System.out.println("Reading first vector...");
                    ByteBuffer vectorBuffer = ByteBuffer.wrap(header, 4, dimensions * componentBytes);
                    vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    
                    System.out.println("First few vector components:");
                    for (int i = 0; i < Math.min(10, dimensions); i++) {
                        if (vectorBuffer.remaining() >= 4) {
                            float value = vectorBuffer.getFloat();
                            System.out.printf("  [%d]: %.6f\n", i, value);
                        }
                    }
                }
            }
            
        } catch (IOException e) {
            System.out.println("Error analyzing file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}