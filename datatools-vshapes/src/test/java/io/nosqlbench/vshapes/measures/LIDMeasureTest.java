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

package io.nosqlbench.vshapes.measures;

import io.nosqlbench.vshapes.TestVectorSpace;
import io.nosqlbench.vshapes.VectorUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

public class LIDMeasureTest {

    @TempDir
    Path tempDir;

    @Test
    public void testLIDMeasureBasic() {
        LIDMeasure measure = new LIDMeasure(3);
        assertEquals("LID", measure.getMnemonic());
        assertEquals(0, measure.getDependencies().length);
        assertEquals(LIDMeasure.LIDResult.class, measure.getResultClass());
    }

    @Test
    public void testLIDComputation() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        LIDMeasure measure = new LIDMeasure(3);
        
        LIDMeasure.LIDResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertNotNull(result);
        assertEquals(vectorSpace.getVectorCount(), result.getVectorCount());
        assertEquals(3, result.k);
        
        // Check that all LID values are positive (or NaN for edge cases)
        for (int i = 0; i < result.lidValues.length; i++) {
            double lid = result.getLID(i);
            assertTrue(lid >= 0 || Double.isNaN(lid), "LID should be non-negative or NaN");
        }
        
        // Statistics should be computed
        assertNotNull(result.statistics);
        assertTrue(result.statistics.mean >= 0 || Double.isNaN(result.statistics.mean));
    }

    @Test
    public void testLIDWithHighDimSpace() {
        TestVectorSpace vectorSpace = TestVectorSpace.createHighDimTestSpace();
        LIDMeasure measure = new LIDMeasure(5);
        
        LIDMeasure.LIDResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertNotNull(result);
        assertEquals(vectorSpace.getVectorCount(), result.getVectorCount());
        
        // In higher dimensions, LID values should generally be higher
        // but this depends on the data distribution
        assertTrue(result.statistics.mean > 0);
    }

    @Test
    public void testLIDResultMethods() {
        double[] lidValues = {1.0, 2.0, 3.0, 4.0};
        VectorUtils.Statistics stats = VectorUtils.computeStatistics(lidValues);
        LIDMeasure.LIDResult result = new LIDMeasure.LIDResult(lidValues, stats, 5);
        
        assertEquals(4, result.getVectorCount());
        assertEquals(1.0, result.getLID(0));
        assertEquals(2.0, result.getLID(1));
        assertEquals(3.0, result.getLID(2));
        assertEquals(4.0, result.getLID(3));
        assertEquals(stats, result.getStatistics());
        assertEquals(5, result.k);
    }

    @Test
    public void testLIDResultToString() {
        LIDMeasure.LIDResult result = new LIDMeasure.LIDResult();
        String str = result.toString();
        
        assertTrue(str.contains("LIDResult"));
        assertTrue(str.contains("k=0"));
        assertTrue(str.contains("vectors=0"));
    }

    @Test
    public void testCacheFilename() {
        TestVectorSpace vectorSpace = new TestVectorSpace("test-space");
        LIDMeasure measure = new LIDMeasure(10);
        
        String filename = measure.getCacheFilename(vectorSpace);
        
        assertTrue(filename.contains("lid"));
        assertTrue(filename.contains("k10"));
        assertTrue(filename.contains("test-space"));
        assertTrue(filename.endsWith(".json"));
    }

    @Test
    public void testDefaultK() {
        LIDMeasure measure = new LIDMeasure();
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        
        LIDMeasure.LIDResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertEquals(20, result.k); // Default k should be 20
    }
}