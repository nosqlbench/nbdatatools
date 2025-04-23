package io.nosqlbench.writers;

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

import com.google.auto.service.AutoService;
import io.nosqlbench.readers.DataType;
import io.nosqlbench.readers.Encoding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

/**
 * Writer for the uniform fvec format (float vectors).
 * 
 * The fvec format consists of:
 * - Per vector: 4-byte integer with the vector dimension
 * - Followed by: dimension * 4-byte float values
 * 
 * Each vector must have the same dimension throughout the file.
 */
@AutoService(Writer.class)
@DataType(float[].class)
@Encoding(Encoding.Type.fvec)
public class UniformFvecWriter implements Writer<float[]>, AutoCloseable {
    
    private static final Logger logger = LogManager.getLogger(UniformFvecWriter.class);
    
    private final Path filePath;
    private final String name;
    private final DataOutputStream outputStream;
    private final int dimension;
    private int vectorsWritten = 0;
    private boolean closed = false;
    
    /**
     * Creates a new UniformFvecWriter for writing vectors of fixed dimension.
     *
     * @param filePath The path to the output file
     * @param dimension The dimension of vectors to write
     * @throws IOException If the file cannot be created or opened for writing
     * @throws IllegalArgumentException If dimension is less than or equal to zero
     */
    public UniformFvecWriter(Path filePath, int dimension) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");
        this.name = filePath.getFileName().toString();
        
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive, got: " + dimension);
        }
        
        this.dimension = dimension;
        this.outputStream = new DataOutputStream(new FileOutputStream(filePath.toFile()));
        
        logger.debug("Created UniformFvecWriter for file: {} with dimension: {}", filePath, dimension);
    }
    
    /**
     * Writes a single float vector to the file.
     *
     * @param vector The float vector to write
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during writing
     * @throws IllegalArgumentException If the vector's length doesn't match the expected dimension
     * @throws IllegalStateException If the writer has been closed
     */
    @Override
    public Writer<float[]> write(float[] vector) throws IOException {
        checkClosed();
        
        if (vector == null) {
            throw new IllegalArgumentException("Vector cannot be null");
        }
        
        if (vector.length != dimension) {
            throw new IllegalArgumentException(
                    "Vector dimension mismatch. Expected: " + dimension + ", Got: " + vector.length);
        }
        
        // Write dimension header
        outputStream.writeInt(dimension);
        
        // Write vector data
        for (float value : vector) {
            outputStream.writeFloat(value);
        }
        
        vectorsWritten++;
        return this;
    }
    
    /**
     * Writes a collection of float vectors to the file.
     *
     * @param vectors The collection of float vectors to write
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during writing
     * @throws IllegalArgumentException If any vector's length doesn't match the expected dimension
     * @throws IllegalStateException If the writer has been closed
     */
    public UniformFvecWriter writeAll(Collection<float[]> vectors) throws IOException {
        checkClosed();
        
        if (vectors == null) {
            throw new IllegalArgumentException("Vectors collection cannot be null");
        }
        
        for (float[] vector : vectors) {
            write(vector);
        }
        
        return this;
    }
    
    /**
     * Optimized method to write an array of vectors using a single ByteBuffer.
     * This is more efficient for writing large batches of vectors.
     *
     * @param vectors Array of float vectors to write
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during writing
     * @throws IllegalArgumentException If any vector's length doesn't match the expected dimension
     * @throws IllegalStateException If the writer has been closed
     */
    public UniformFvecWriter writeAllBulk(float[][] vectors) throws IOException {
        checkClosed();
        
        if (vectors == null) {
            throw new IllegalArgumentException("Vectors array cannot be null");
        }
        
        // Calculate total buffer size: for each vector, we need space for dimension + vector data
        int bufferSize = vectors.length * (4 + (dimension * 4));
        // Use BIG_ENDIAN to match DataOutputStream's format which is used in other methods
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.BIG_ENDIAN);
        
        for (float[] vector : vectors) {
            if (vector == null || vector.length != dimension) {
                throw new IllegalArgumentException(
                        "Invalid vector. Expected dimension: " + dimension + 
                        ", Got: " + (vector == null ? "null" : vector.length));
            }
            
            buffer.putInt(dimension);
            
            for (float value : vector) {
                buffer.putFloat(value);
            }
        }
        
        // Reset buffer position to beginning for reading
        buffer.flip();
        
        // Write the entire buffer to the output stream
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        outputStream.write(data);
        
        vectorsWritten += vectors.length;
        return this;
    }
    
    /**
     * Creates a vector of specified dimension with all elements set to the given value.
     *
     * @param size The dimension of the vector
     * @param value The value to fill the vector with
     * @return A new float array of the specified size filled with the value
     */
    public static float[] createUniformVector(int size, float value) {
        float[] vector = new float[size];
        java.util.Arrays.fill(vector, value);
        return vector;
    }
    
    /**
     * Creates a sequence vector with values ranging from start to start + size - 1.
     *
     * @param size The dimension of the vector
     * @param start The starting value
     * @return A new float array with sequential values
     */
    public static float[] createSequenceVector(int size, float start) {
        float[] vector = new float[size];
        for (int i = 0; i < size; i++) {
            vector[i] = start + i;
        }
        return vector;
    }
    
    /**
     * Returns the number of vectors written so far.
     *
     * @return The count of vectors written
     */
    public int getVectorsWritten() {
        return vectorsWritten;
    }
    
    /**
     * Returns the dimension of vectors being written.
     *
     * @return The vector dimension
     */
    public int getDimension() {
        return dimension;
    }
    
    /**
     * Returns the path to the output file.
     *
     * @return The file path
     */
    public Path getFilePath() {
        return filePath;
    }
    
    /**
     * Returns the name of this writer (typically the filename).
     *
     * @return The writer name
     */
    @Override
    public String getName() {
        return name;
    }
    
    /**
     * Flushes any buffered data to the underlying file.
     *
     * @return This writer instance for method chaining
     * @throws IOException If an I/O error occurs during flushing
     * @throws IllegalStateException If the writer has been closed
     */
    public UniformFvecWriter flush() throws IOException {
        checkClosed();
        outputStream.flush();
        return this;
    }
    
    /**
     * Closes the writer and releases resources.
     * After closing, no more vectors can be written.
     *
     * @throws IOException If an I/O error occurs during closing
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            outputStream.close();
            closed = true;
            logger.debug("Closed UniformFvecWriter for file: {} after writing {} vectors", 
                    filePath, vectorsWritten);
        }
    }

    /**
     * Checks if the writer has been closed and throws an exception if it has.
     *
     * @throws IllegalStateException If the writer has been closed
     */
    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
    }
}
