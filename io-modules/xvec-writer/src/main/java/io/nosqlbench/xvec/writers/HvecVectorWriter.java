package io.nosqlbench.xvec.writers;

import io.nosqlbench.nbvectors.api.fileio.VectorFileStore;
import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

/**
 * VectorWriter implementation for float[] vectors in hvec format (half-precision floating point).
 * Each vector is written as a little-endian int dimension followed by
 * a little-endian buffer of IEEE 754-2008 binary16 (half-precision) values.
 * 
 * This implementation converts standard Java float values to half-precision (16-bit)
 * floating point values using the IEEE 754-2008 binary16 format.
 */
@DataType(float[].class)
@Encoding(FileType.xvec)
public class HvecVectorWriter implements VectorFileStore<float[]> {

    private BufferedOutputStream outputStream;
    private Integer dimension;
    private ByteBuffer buffer;

    /**
     * Default constructor required for SPI.
     */
    public HvecVectorWriter() {
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
            // Allocate buffer if needed - 4 bytes for dimension (int) and 2 bytes per half-precision float
            if (buffer == null || buffer.capacity() < 4 + dimension * 2) {
                buffer = ByteBuffer.allocate(4 + dimension * 2).order(ByteOrder.LITTLE_ENDIAN);
            } else {
                buffer.clear();
            }

            // Write dimension as little-endian int
            buffer.putInt(dimension);

            // Write vector data as little-endian half-precision floats
            for (float value : data) {
                short halfFloat = floatToHalf(value);
                buffer.putShort(halfFloat);
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

    /**
     * Converts a 32-bit float to a 16-bit half-precision float.
     * Implementation follows the IEEE 754-2008 binary16 format.
     *
     * @param f The 32-bit float value to convert
     * @return The 16-bit half-precision float value as a short
     */
    private short floatToHalf(float f) {
        // Get the bits from the float
        int bits = Float.floatToIntBits(f);
        
        // Extract components
        int sign = (bits >>> 31) & 0x1;
        int exponent = (bits >>> 23) & 0xFF;
        int mantissa = bits & 0x7FFFFF;
        
        // Special cases: NaN and Infinity
        if (exponent == 0xFF) {
            if (mantissa != 0) {
                // NaN
                return (short) 0x7E00; // Half-precision NaN
            } else {
                // Infinity
                return (short) ((sign << 15) | 0x7C00); // Half-precision Infinity with sign
            }
        }
        
        // Adjust exponent: IEEE float exponent bias is 127, half-precision is 15
        int newExponent = exponent - 127 + 15;
        
        // Handle overflow
        if (newExponent >= 31) {
            return (short) ((sign << 15) | 0x7C00); // Infinity with sign
        }
        
        // Handle underflow
        if (newExponent <= 0) {
            // Denormalized or zero
            if (newExponent < -10) {
                return (short) (sign << 15); // Zero with sign
            }
            
            // Denormalized number
            mantissa = (mantissa | 0x800000) >> (14 - newExponent);
            return (short) ((sign << 15) | mantissa);
        }
        
        // Normalized number
        int newMantissa = mantissa >> 13; // Truncate to 10 bits
        
        // Compose the half-precision float
        return (short) ((sign << 15) | (newExponent << 10) | newMantissa);
    }
}