package io.nosqlbench.xvec.writers;

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


import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.DataType;
import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.services.FileExtension;
import io.nosqlbench.nbdatatools.api.services.FileType;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/// VectorWriter implementation for double[] vectors in dvec format.
/// Each vector is written as a little-endian int dimension followed by
/// a little-endian buffer of double values.
/// 
/// ```
/// +----------------+------------------+
/// | dimension (4B) | double[] values  |
/// +----------------+------------------+
/// ```
@DataType(double[].class)
@Encoding(FileType.xvec)
@FileExtension({".dvec",".dvecs"})
public class DvecVectorWriter implements VectorFileStreamStore<double[]> {

    private BufferedOutputStream outputStream;
    private Integer dimension;
    private ByteBuffer buffer;

    /// Default constructor required for SPI.
    public DvecVectorWriter() {
    }

    /// Opens a file for writing double[] vectors in dvec format.
    /// @param path The path to the file to write to
    @Override
    public void open(Path path) {
        try {
            this.outputStream = new BufferedOutputStream(new FileOutputStream(path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file for writing: " + path, e);
        }
    }

    /// Writes a double[] vector to the file in dvec format.
    /// The first vector written determines the dimension for all subsequent vectors.
    /// @param data The double[] vector to write
    /// @throws IllegalArgumentException if data is null or has a different dimension than previously written vectors
    @Override
    public void write(double[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        // Check and set dimension
        if (dimension == null) {
            dimension = data.length;
        } else if (dimension != data.length) {
            throw new IllegalArgumentException(
                "Vector dimension mismatch. Expected: " + dimension + ", Got: " + data.length);
        }

        try {
            // Allocate buffer if needed - 4 bytes for dimension (int) and 8 bytes per double
            if (buffer == null || buffer.capacity() < 4 + dimension * 8) {
                buffer = ByteBuffer.allocate(4 + dimension * 8).order(ByteOrder.LITTLE_ENDIAN);
            } else {
                buffer.clear();
            }

            // Write dimension as little-endian int
            buffer.putInt(dimension);

            // Write vector data as little-endian doubles
            for (double value : data) {
                buffer.putDouble(value);
            }

            // Write to file
            buffer.flip();
            outputStream.write(buffer.array(), 0, buffer.limit());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector data", e);
        }
    }

    /// Closes the output stream.
    /// This should be called when done writing vectors to ensure all data is flushed and resources are released.
    /// @throws RuntimeException if there is an error closing the output stream
    @Override
    public void close() {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close output stream", e);
            }
        }
    }
}
