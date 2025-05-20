package io.nosqlbench.readers;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvJsonArrayStreamerTest {

    @TempDir
    Path tempDir;

    @Test
    void testJsonArrayInFirstColumn() throws IOException {
        Path testFile = tempDir.resolve("vectors_first_column.csv");
        Files.writeString(testFile, """
            "[1.0, 2.0, 3.0]",label1,description1
            "[4.0, 5.0, 6.0]",label2,description2
            "[7.0, 8.0, 9.0]",label3,description3"""
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        Iterator<float[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext());
        float[] vector1 = iterator.next();
        assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, vector1, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector2 = iterator.next();
        assertArrayEquals(new float[]{4.0f, 5.0f, 6.0f}, vector2, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector3 = iterator.next();
        assertArrayEquals(new float[]{7.0f, 8.0f, 9.0f}, vector3, 0.0001f);
        
        assertFalse(iterator.hasNext());
    }
    
    @Test
    void testJsonArrayInMiddleColumn() throws IOException {
        Path testFile = tempDir.resolve("vectors_middle_column.csv");
        Files.writeString(testFile, """
            id1,"[1.0, 2.0, 3.0]",meta1
            id2,"[4.0, 5.0, 6.0]",meta2
            id3,"[7.0, 8.0, 9.0]",meta3
            """
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        Iterator<float[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext());
        float[] vector1 = iterator.next();
        assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, vector1, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector2 = iterator.next();
        assertArrayEquals(new float[]{4.0f, 5.0f, 6.0f}, vector2, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector3 = iterator.next();
        assertArrayEquals(new float[]{7.0f, 8.0f, 9.0f}, vector3, 0.0001f);
    }
    
    @Test
    void testHeaderWithColumnNames() throws IOException {
        Path testFile = tempDir.resolve("vectors_with_header.csv");
        Files.writeString(testFile, """
            id,vector,description
            id1,"[1.0, 2.0, 3.0]",desc1
            id2,"[4.0, 5.0, 6.0]",desc2"""
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        Iterator<float[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext());
        float[] vector1 = iterator.next();
        assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, vector1, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector2 = iterator.next();
        assertArrayEquals(new float[]{4.0f, 5.0f, 6.0f}, vector2, 0.0001f);
        
        assertFalse(iterator.hasNext());
    }
    
    @Test
    void testHeaderDetectionWithComplexStructure() throws IOException {
        Path testFile = tempDir.resolve("vectors_complex_header.csv");
        Files.writeString(testFile, """
            document_id,embedding_vector,confidence_score,category
            doc123,"[1.5, 2.5, 3.5, 4.5]",0.98,science
            doc456,"[5.5, 6.5, 7.5, 8.5]",0.87,history"""
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        Iterator<float[]> iterator = streamer.iterator();
        
        assertTrue(iterator.hasNext());
        float[] vector1 = iterator.next();
        assertArrayEquals(new float[]{1.5f, 2.5f, 3.5f, 4.5f}, vector1, 0.0001f);
        
        assertTrue(iterator.hasNext());
        float[] vector2 = iterator.next();
        assertArrayEquals(new float[]{5.5f, 6.5f, 7.5f, 8.5f}, vector2, 0.0001f);
        
        assertFalse(iterator.hasNext());
    }
    
    @Test
    void testJsonArrayWithVariableDimensions() throws IOException {
        Path testFile = tempDir.resolve("vectors_variable_dimensions.csv");
        Files.writeString(testFile, """
            id1,"[1.0, 2.0, 3.0]",meta1
            id2,"[4.0, 5.0, 6.0, 7.0]",meta2
            id3,"[8.0, 9.0]",meta3"""
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        Iterator<float[]> iterator = streamer.iterator();
        
        List<float[]> vectors = new ArrayList<>();
        while (iterator.hasNext()) {
            vectors.add(iterator.next());
        }
        
        assertEquals(3, vectors.size(), "Should read 3 vectors with different dimensions");
        
        assertArrayEquals(new float[]{1.0f, 2.0f, 3.0f}, vectors.get(0), 0.0001f);
        assertArrayEquals(new float[]{4.0f, 5.0f, 6.0f, 7.0f}, vectors.get(1), 0.0001f);
        assertArrayEquals(new float[]{8.0f, 9.0f}, vectors.get(2), 0.0001f);
    }
    
    @Test
    void testMultipleIterators() throws IOException {
        Path testFile = tempDir.resolve("multiple_iterators.csv");
        Files.writeString(testFile, """
            "[1.0, 2.0]",A
            "[3.0, 4.0]",B
            "[5.0, 6.0]",C"""
        );
        
        CsvJsonArrayStreamer streamer = new CsvJsonArrayStreamer(testFile);
        
        // First iterator should get all the data
        Iterator<float[]> iterator1 = streamer.iterator();
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{1.0f, 2.0f}, iterator1.next(), 0.0001f);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{3.0f, 4.0f}, iterator1.next(), 0.0001f);
        assertTrue(iterator1.hasNext());
        assertArrayEquals(new float[]{5.0f, 6.0f}, iterator1.next(), 0.0001f);
        assertFalse(iterator1.hasNext());
        
        // Second iterator should start from the beginning again
        Iterator<float[]> iterator2 = streamer.iterator();
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{1.0f, 2.0f}, iterator2.next(), 0.0001f);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{3.0f, 4.0f}, iterator2.next(), 0.0001f);
        assertTrue(iterator2.hasNext());
        assertArrayEquals(new float[]{5.0f, 6.0f}, iterator2.next(), 0.0001f);
        assertFalse(iterator2.hasNext());
    }
}
