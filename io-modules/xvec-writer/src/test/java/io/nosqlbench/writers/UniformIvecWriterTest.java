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

package io.nosqlbench.writers;

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
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UniformIvecWriterTest {
    
    @TempDir
    Path tempDir;
    
    private Path testFilePath;
    private UniformIvecWriter writer;
    private static final int TEST_DIMENSION = 5;
    
    @BeforeEach
    void setUp() throws IOException {
        testFilePath = tempDir.resolve("test.ivec");
        writer = new UniformIvecWriter(testFilePath, TEST_DIMENSION);
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
        assertThat(writer.getName()).isEqualTo("test.ivec");
        assertThat(writer.getDimension()).isEqualTo(TEST_DIMENSION);
        assertThat(writer.getVectorsWritten()).isZero();
    }
    
    @Test
    void shouldWriteSingleVector() throws IOException {
        int[] vector = {1, 2, 3, 4, 5};
        writer.write(vector);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(1);
        assertThat(Files.exists(testFilePath)).isTrue();
        assertThat(Files.size(testFilePath)).isEqualTo(4 + (4 * TEST_DIMENSION)); // 4 bytes for dim + 4 bytes per int
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            int dimension = dis.readInt();
            assertThat(dimension).isEqualTo(TEST_DIMENSION);
            
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readInt()).isEqualTo(i + 1);
            }
        }
    }
    
    @Test
    void shouldWriteMultipleVectors() throws IOException {
        List<int[]> vectors = new ArrayList<>();
        vectors.add(new int[]{1, 2, 3, 4, 5});
        vectors.add(new int[]{6, 7, 8, 9, 10});
        
        writer.writeAll(vectors);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(2);
        assertThat(Files.size(testFilePath)).isEqualTo(2 * (4 + (4 * TEST_DIMENSION)));
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            // First vector
            assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readInt()).isEqualTo(i + 1);
            }
            
            // Second vector
            assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
            for (int i = 0; i < TEST_DIMENSION; i++) {
                assertThat(dis.readInt()).isEqualTo(i + 6);
            }
        }
    }
    
    @Test
    void shouldWriteVectorsBulk() throws IOException {
        int[][] vectors = new int[][]{
            {1, 2, 3, 4, 5},
            {6, 7, 8, 9, 10},
            {11, 12, 13, 14, 15}
        };
        
        writer.writeAllBulk(vectors);
        
        assertThat(writer.getVectorsWritten()).isEqualTo(3);
        assertThat(Files.size(testFilePath)).isEqualTo(3 * (4 + (4 * TEST_DIMENSION)));
        
        // Verify file contents
        try (DataInputStream dis = new DataInputStream(new FileInputStream(testFilePath.toFile()))) {
            for (int v = 0; v < vectors.length; v++) {
                assertThat(dis.readInt()).isEqualTo(TEST_DIMENSION);
                for (int i = 0; i < TEST_DIMENSION; i++) {
                    assertThat(dis.readInt()).isEqualTo(vectors[v][i]);
                }
            }
        }
    }
    
    @Test
    void shouldRejectVectorsWithWrongDimension() throws IOException {
        int[] wrongSizeVector = {1, 2, 3}; // Dimension 3 != 5
        
        assertThatThrownBy(() -> writer.write(wrongSizeVector))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Vector dimension mismatch");
    }
    
    @Test
    void shouldCreateHelperVectors() {
        // Test uniform vector
        int[] uniform = UniformIvecWriter.createUniformVector(4, 42);
        assertThat(uniform).hasSize(4);
        assertThat(uniform).containsOnly(42);
        
        // Test sequence vector
        int[] sequence = UniformIvecWriter.createSequenceVector(4, 10);
        assertThat(sequence).hasSize(4);
        assertThat(sequence).containsExactly(10, 11, 12, 13);
    }
    
    @Test
    void shouldRejectWritesAfterClose() throws IOException {
        writer.close();
        
        int[] vector = {1, 2, 3, 4, 5};
        
        assertThatThrownBy(() -> writer.write(vector))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.writeAll(List.of(vector)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.writeAllBulk(new int[][]{vector}))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
        
        assertThatThrownBy(() -> writer.flush())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Writer is closed");
    }
}
