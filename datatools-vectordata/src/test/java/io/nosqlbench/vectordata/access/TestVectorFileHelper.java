package io.nosqlbench.vectordata.access;

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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

/// Shared utilities for creating test vector files in xvec format.
///
/// Each xvec record is: `[dim: i32 LE][element_0 .. element_{dim-1}: T LE]`
///
/// Vector values follow a simple pattern for easy assertion:
/// `vector[i][j] = i * dim + j` (cast to the target type).
public final class TestVectorFileHelper {

    private TestVectorFileHelper() {}

    /// Creates a test `.fvec` file with predictable float vector values.
    ///
    /// @param dir directory to create the file in
    /// @param filename name of the file (e.g., "base.fvec")
    /// @param dim dimensionality of each vector
    /// @param count number of vectors
    /// @return path to the created file
    public static Path createFvec(Path dir, String filename, int dim, int count) throws IOException {
        Path path = dir.resolve(filename);
        int recordSize = 4 + dim * 4;
        ByteBuffer buf = ByteBuffer.allocate(recordSize * count).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buf.putInt(dim);
            for (int j = 0; j < dim; j++) {
                buf.putFloat((float) (i * dim + j));
            }
        }
        buf.flip();
        Files.write(path, buf.array());
        return path;
    }

    /// Creates a test `.ivec` file with predictable int vector values.
    ///
    /// @param dir directory to create the file in
    /// @param filename name of the file (e.g., "neighbors.ivec")
    /// @param dim dimensionality of each vector
    /// @param count number of vectors
    /// @return path to the created file
    public static Path createIvec(Path dir, String filename, int dim, int count) throws IOException {
        Path path = dir.resolve(filename);
        int recordSize = 4 + dim * 4;
        ByteBuffer buf = ByteBuffer.allocate(recordSize * count).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buf.putInt(dim);
            for (int j = 0; j < dim; j++) {
                buf.putInt(i * dim + j);
            }
        }
        buf.flip();
        Files.write(path, buf.array());
        return path;
    }

    /// Creates a test `.mvec` (minivec, f16 half-precision) file with predictable f16 vector values.
    ///
    /// Values are stored as IEEE 754 binary16 (half-precision).
    /// The pattern is `vector[i][j] = i * dim + j` converted to f16.
    ///
    /// @param dir directory to create the file in
    /// @param filename name of the file (e.g., "base.mvec")
    /// @param dim dimensionality of each vector
    /// @param count number of vectors
    /// @return path to the created file
    public static Path createMvec(Path dir, String filename, int dim, int count) throws IOException {
        Path path = dir.resolve(filename);
        int recordSize = 4 + dim * 2;
        ByteBuffer buf = ByteBuffer.allocate(recordSize * count).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buf.putInt(dim);
            for (int j = 0; j < dim; j++) {
                buf.putShort(floatToFloat16((float) (i * dim + j)));
            }
        }
        buf.flip();
        Files.write(path, buf.array());
        return path;
    }

    /// Creates a `dataset.yaml` file with the given content.
    ///
    /// @param dir directory to create the file in
    /// @param yamlContent the YAML content
    /// @return path to the created file
    public static Path createDatasetYaml(Path dir, String yamlContent) throws IOException {
        Path path = dir.resolve("dataset.yaml");
        Files.writeString(path, yamlContent);
        return path;
    }

    /// Returns raw bytes for an fvec file (for merkle hashing).
    ///
    /// @param dim dimensionality
    /// @param count number of vectors
    /// @return byte array of the entire file content
    public static byte[] fvecBytes(int dim, int count) {
        int recordSize = 4 + dim * 4;
        ByteBuffer buf = ByteBuffer.allocate(recordSize * count).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            buf.putInt(dim);
            for (int j = 0; j < dim; j++) {
                buf.putFloat((float) (i * dim + j));
            }
        }
        return buf.array();
    }

    /// Converts a float to IEEE 754 binary16 (half-precision) representation.
    ///
    /// @param f the float value
    /// @return the f16 bits as a short
    private static short floatToFloat16(float f) {
        int bits = Float.floatToIntBits(f);
        int sign = (bits >>> 16) & 0x8000;
        int val = (bits & 0x7FFFFFFF) + 0x1000;

        if (val >= 0x47800000) {
            // overflow → infinity
            if ((bits & 0x7FFFFFFF) >= 0x47800000) {
                if (val < 0x7F800000) return (short) (sign | 0x7C00);
                return (short) (sign | 0x7C00 | ((bits & 0x007FFFFF) >>> 13));
            }
            return (short) (sign | 0x7BFF);
        }
        if (val >= 0x38800000) {
            return (short) (sign | ((val - 0x38000000) >>> 13));
        }
        if (val < 0x33000000) {
            return (short) sign;
        }
        val = (bits & 0x7FFFFFFF) >>> 23;
        return (short) (sign | (((bits & 0x7FFFFF) | 0x800000) + (0x800000 >>> (val - 102)) >>> (126 - val)));
    }
}
