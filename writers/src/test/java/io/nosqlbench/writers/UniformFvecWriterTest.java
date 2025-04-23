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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UniformFvecWriterTest {
    
    @TempDir
    Path tempDir;
    
    private Path testFilePath;
    private UniformFvecWriter writer;
    private static final int TEST_DIMENSION = 5;
    
    @BeforeEach
    void setUp() throws IOException {
        testFilePath = tempDir.resolve("test.fvec");
        writer = new UniformFvecWriter(testFilePath, TEST_DIMENSION);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
    
    @Test
    void shouldCreateFileWithCorrectPath() {
        assertThat(writer.getFilePath()).isEqualTo(testFilePath);
        assertThat(writer.getName()).isEqualTo("test.fvec");
        assertThat(writer.getDimension()).isEqualTo(TEST_DIMENSION);
        assertThat(writer.getVectorsWritten()).isZero();
    }
    
    @Test
    void shouldWriteSingleVector() throws IOException {
        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        writer.write(vector);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(1);
        assertThat(Files.exists(testFilePath)).isTrue();
        assertThat(Files.size(testFilePath)).isEqualTo(4 + (4 * TEST_DIMENSION)); // 4 bytes for dim + 4 bytes per float
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            int dimension = dis.readInt();
            assertThat(dimension).isEqualTo(TEST_DIMENSION);
            
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readFloat()).isEqualTo(i + 1.0f);
            }
        }
    }
    
    @Test
    void shouldWriteMultipleVectors() throws IOException {
        List<float[]> vectors = new ArrayList<>();
        vectors.add(new float[]{1.0f, 2.0f, 3.0f, 4.0f, 5.0f});
        vectors.add(new float[]{6.0f, 7.0f, 8.0f, 9.0f, 10.0f});
        
        writer.writeAll(vectors);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(2);
        assertThat(Files.size(testFilePath)).isEqualTo(2 * (4 + (4 * TEST_DIMENSION)));
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            // First vector
            assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readFloat()).isEqualTo(i + 1.0f);
            }
            
            // Second vector
            assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readFloat()).isEqualTo(i + 6.0f);
            }
        }
    }
    
    @Test
    void shouldWriteVectorsBulk() throws IOException {
        float[][] vectors = new float[][]{
            {1.0f, 2.0f, 3.0f, 4.0f, 5.0f},
            {6.0f, 7.0f, 8.0f, 9.0f, 10.0f},
            {11.0f, 12.0f, 13.0f, 14.0f, 15.0f}
        };
        
        writer.writeAllBulk(vectors);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(3);
        assertThat(Files.size(testFilePath)).isEqualTo(3 * (4 + (4 * TEST_DIMENSION)));
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            for (int v = 0; v < vectors.length; v++) {
                assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
                for (int i = 0; i < TEST_DIMENSION; i++) {
                    assertThat(dis.readFloat()).isEqualTo(vectors[v][i]);
                }
            }
        }
    }
    
    @Test
    void shouldRejectVectorsWithWrongDimension() throws IOException {
        float[] wrongSizeVector = {1.0f, 2.0f, 3.0f}; // Dimension 3 != 5
        
        assertThatThrownBy(() -> writer.write(wrongSizeVector))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Vector dimension mismatch");
    }
    
    @Test
    void shouldCreateHelperVectors() {
        // Test uniform vector
        float[] uniform = UniformFvecWriter.createUniformVector(4, 42.5f);
        assertThat(uniform).hasSize(4);
        assertThat(uniform).containsOnly(42.5f);
        
        // Test sequence vector
        float[] sequence = UniformFvecWriter.createSequenceVector(4, 10.5f);
        assertThat(sequence).hasSize(4);
        assertThat(sequence).containsExactly(10.5f, 11.5f, 12.5f, 13.5f);
    }
    
    @Test
    void shouldRejectWritesAfterClose() throws IOException {
        writer.close();
        
        float[] vector = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        
        assertThatThrownBy(() -> writer.write(vector))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.writeAll(List.of(vector)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.writeAllBulk(new float[][]{vector}))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.flush())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
    }
}
