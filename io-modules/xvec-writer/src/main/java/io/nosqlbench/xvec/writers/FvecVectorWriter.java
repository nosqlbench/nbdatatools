package io.nosqlbench.xvec.writers;

import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.fileio.VectorWriter;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * VectorWriter implementation for float[] vectors in fvec format.
 * Each vector is written as a little-endian int dimension followed by
 * a little-endian buffer of float values.
 */
@DataType(float[].class)
@Encoding(FileType.xvec)
public class FvecVectorWriter implements VectorWriter<float[]> {

    private BufferedOutputStream outputStream;
    private Integer dimension;
    private ByteBuffer buffer;

    /**
     * Default constructor required for SPI.
     */
    public FvecVectorWriter() {
    }

    @Override
    public void open(Path path) {
        try {
            this.outputStream = new BufferedOutputStream(new FileOutputStream(path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open file for writing: " + path, e);
        }
    }

    @Override
    public void write(float[] data) {
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
            // Allocate buffer if needed - 4 bytes for dimension (int) and 4 bytes per float
            if (buffer == null || buffer.capacity() < 4 + dimension * 4) {
                buffer = ByteBuffer.allocate(4 + dimension * 4).order(ByteOrder.LITTLE_ENDIAN);
            } else {
                buffer.clear();
            }

            // Write dimension as little-endian int
            buffer.putInt(dimension);

            // Write vector data as little-endian floats
            for (float value : data) {
                buffer.putFloat(value);
            }

            // Write to file
            buffer.flip();
            outputStream.write(buffer.array(), 0, buffer.limit());
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector data", e);
        }
    }

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