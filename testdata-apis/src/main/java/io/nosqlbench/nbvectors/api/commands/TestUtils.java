package io.nosqlbench.nbvectors.api.commands;

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


import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for creating test ivec files with various configurations.
 */
public class TestUtils {
    
    /**
     * Creates a test ivec file with the specified number of vectors and dimensions.
     * All vectors will have the same dimension.
     * 
     * @param filePath The path where the ivec file should be created
     * @param numVectors The number of vectors to generate
     * @param dimensions The dimension for each vector
     * @param seed Random seed for reproducible values
     * @return The actual file path that was created
     * @throws IOException If an error occurs while writing the file
     */
    public static Path createUniformIvecFile(Path filePath, int numVectors, int dimensions, long seed) 
            throws IOException {
        Random random = new Random(seed);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write vectors with uniform dimensions
            for (int i = 0; i < numVectors; i++) {
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions; j++) {
                    // For single-dimension index vectors, generate sequential indices
                    if (dimensions == 1) {
                        dos.writeInt(i);
                    } else {
                        dos.writeInt(random.nextInt(1000));
                    }
                }
            }
        }
        
        return filePath;
    }
    
    /**
     * Creates a test fvec file with the specified number of vectors and dimensions.
     * All vectors will have the same dimension.
     * 
     * @param filePath The path where the fvec file should be created
     * @param numVectors The number of vectors to generate
     * @param dimensions The dimension for each vector
     * @param seed Random seed for reproducible values
     * @return The actual file path that was created
     * @throws IOException If an error occurs while writing the file
     */
    public static Path createUniformFvecFile(Path filePath, int numVectors, int dimensions, long seed) 
            throws IOException {
        Random random = new Random(seed);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write vectors with uniform dimensions
            for (int i = 0; i < numVectors; i++) {
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions; j++) {
                    dos.writeFloat(random.nextFloat());
                }
            }
        }
        
        return filePath;
    }
    
    /**
     * Creates a test ivec file with the specified number of vectors where
     * each vector can have a different dimension.
     * 
     * @param filePath The path where the ivec file should be created
     * @param numVectors The number of vectors to generate
     * @param minDimensions The minimum dimension for a vector
     * @param maxDimensions The maximum dimension for a vector
     * @param seed Random seed for reproducible values
     * @return The actual file path that was created
     * @throws IOException If an error occurs while writing the file
     */
    public static Path createVariableDimensionIvecFile(Path filePath, int numVectors, 
                                                     int minDimensions, int maxDimensions, long seed) 
            throws IOException {
        Random random = new Random(seed);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write vectors with varying dimensions
            for (int i = 0; i < numVectors; i++) {
                int dimensions = minDimensions + random.nextInt(maxDimensions - minDimensions + 1);
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions; j++) {
                    dos.writeInt(random.nextInt(1000));
                }
            }
        }
        
        return filePath;
    }
    
    /**
     * Creates a test ivec file where all vectors have the same dimension except for
     * one specified vector that has a different dimension.
     * 
     * @param filePath The path where the ivec file should be created
     * @param numVectors The number of vectors to generate
     * @param uniformDimension The dimension for most vectors
     * @param outlierIndex The index of the vector that should have a different dimension
     * @param outlierDimension The dimension for the outlier vector
     * @param seed Random seed for reproducible values
     * @return The actual file path that was created
     * @throws IOException If an error occurs while writing the file
     */
    public static Path createOneOutlierDimensionIvecFile(Path filePath, int numVectors,
                                                       int uniformDimension, int outlierIndex, 
                                                       int outlierDimension, long seed) 
            throws IOException {
        Random random = new Random(seed);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write vectors with one outlier dimension
            for (int i = 0; i < numVectors; i++) {
                int dimensions = (i == outlierIndex) ? outlierDimension : uniformDimension;
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions; j++) {
                    dos.writeInt(random.nextInt(1000));
                }
            }
        }
        
        return filePath;
    }
    
    /**
     * Creates a corrupt ivec file where the file size doesn't match what would be
     * expected based on the vector dimensions.
     * 
     * @param filePath The path where the ivec file should be created
     * @param seed Random seed for reproducible values
     * @return The actual file path that was created
     * @throws IOException If an error occurs while writing the file
     */
    public static Path createCorruptIvecFile(Path filePath, boolean truncate, long seed) 
            throws IOException {
        Random random = new Random(seed);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            // Write a few normal vectors
            int dimensions = 10;
            for (int i = 0; i < 3; i++) {
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions; j++) {
                    dos.writeInt(random.nextInt(1000));
                }
            }
            
            // Add a partial vector to corrupt the file
            if (truncate) {
                // Write dimension but not all the values
                dos.writeInt(dimensions);
                for (int j = 0; j < dimensions - 2; j++) {
                    dos.writeInt(random.nextInt(1000));
                }
            } else {
                // Write extra bytes
                for (int j = 0; j < 3; j++) {
                    dos.writeInt(random.nextInt(1000));
                }
            }
        }
        
        return filePath;
    }
    
    /**
     * Reads an ivec file and returns the expected vectors as float arrays.
     * This provides a reference implementation for testing.
     * 
     * @param filePath The path to the ivec file
     * @return List of float arrays representing the vectors
     * @throws IOException If an error occurs while reading
     */
    public static List<float[]> readExpectedVectors(Path filePath) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        
        try (java.io.DataInputStream dis = new java.io.DataInputStream(Files.newInputStream(filePath))) {
            while (dis.available() > 0) {
                int dimension = dis.readInt();
                float[] vector = new float[dimension];
                
                for (int i = 0; i < dimension; i++) {
                    vector[i] = (float) dis.readInt();
                }
                
                vectors.add(vector);
            }
        }
        
        return vectors;
    }
    
    /**
     * Compares two float arrays for equality within a small epsilon.
     * 
     * @param a First array
     * @param b Second array
     * @return True if arrays have same length and all elements are equal within epsilon
     */
    public static boolean arraysEqual(float[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        }
        
        float epsilon = 1e-6f;
        for (int i = 0; i < a.length; i++) {
            if (Math.abs(a[i] - b[i]) > epsilon) {
                return false;
            }
        }
        
        return true;
    }
}
