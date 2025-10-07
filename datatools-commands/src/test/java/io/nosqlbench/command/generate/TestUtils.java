package io.nosqlbench.command.generate;

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


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Random;

/// Utility class for creating test ivec files with various configurations.
public class TestUtils {

    /// Construct a TestUtils instance.
    ///
    /// Private constructor to prevent instantiation of this utility class.
    private TestUtils() {}

    /// Creates a test ivec file with the specified number of vectors and dimensions.
    /// All vectors will have the same dimension.
    /// 
    /// @param filePath The path where the ivec file should be created
    /// @param numVectors The number of vectors to generate
    /// @param dimensions The dimension for each vector
    /// @param seed Random seed for reproducible values
    /// @return The actual file path that was created
    /// @throws IOException If an error occurs while writing the file
    public static Path createUniformIvecFile(Path filePath, int numVectors, int dimensions, long seed) 
            throws IOException {
        Random random = new Random(seed);

        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            // Write vectors with uniform dimensions
            for (int i = 0; i < numVectors; i++) {
                // Allocate a ByteBuffer with little-endian byte order
                ByteBuffer buffer = ByteBuffer.allocate(4 + dimensions * 4).order(ByteOrder.LITTLE_ENDIAN);

                // Write dimension
                buffer.putInt(dimensions);

                // Write vector data
                for (int j = 0; j < dimensions; j++) {
                    // For single-dimension index vectors, generate sequential indices
                    if (dimensions == 1) {
                        buffer.putInt(i);
                    } else {
                        buffer.putInt(random.nextInt(1000));
                    }
                }

                // Write buffer to file
                fos.write(buffer.array());
            }
        }

        return filePath;
    }

    /// Creates a test fvec file with the specified number of vectors and dimensions.
    /// All vectors will have the same dimension.
    /// 
    /// @param filePath The path where the fvec file should be created
    /// @param numVectors The number of vectors to generate
    /// @param dimensions The dimension for each vector
    /// @param seed Random seed for reproducible values
    /// @return The actual file path that was created
    /// @throws IOException If an error occurs while writing the file
    public static Path createUniformFvecFile(Path filePath, int numVectors, int dimensions, long seed) 
            throws IOException {
        Random random = new Random(seed);

        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            // Write vectors with uniform dimensions
            for (int i = 0; i < numVectors; i++) {
                // Allocate a ByteBuffer with little-endian byte order
                ByteBuffer buffer = ByteBuffer.allocate(4 + dimensions * 4).order(ByteOrder.LITTLE_ENDIAN);

                // Write dimension
                buffer.putInt(dimensions);

                // Write vector data
                for (int j = 0; j < dimensions; j++) {
                    buffer.putFloat(random.nextFloat());
                }

                // Write buffer to file
                fos.write(buffer.array());
            }
        }

        return filePath;
    }
}
