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

public class MarginMeasureTest {

    @TempDir
    Path tempDir;

    @Test
    public void testMarginMeasureBasic() {
        MarginMeasure measure = new MarginMeasure();
        assertEquals("Margin", measure.getMnemonic());
        assertEquals(0, measure.getDependencies().length);
        assertEquals(MarginMeasure.MarginResult.class, measure.getResultClass());
    }

    @Test
    public void testMarginComputationWithClassLabels() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        MarginMeasure measure = new MarginMeasure();
        
        MarginMeasure.MarginResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertNotNull(result);
        assertEquals(vectorSpace.getVectorCount(), result.getVectorCount());
        assertTrue(result.validCount > 0);
        
        // Check that valid margins are positive
        for (int i = 0; i < result.marginValues.length; i++) {
            if (result.isValidMargin(i)) {
                assertTrue(result.getMargin(i) > 0, "Valid margins should be positive");
            }
        }
        
        // Statistics should be computed for valid margins
        assertNotNull(result.statistics);
        if (result.validCount > 0) {
            assertTrue(result.statistics.mean > 0);
        }
    }

    @Test
    public void testMarginWithoutClassLabels() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        vectorSpace.addVector(new float[]{0.0f, 0.0f}); // No class label
        vectorSpace.addVector(new float[]{1.0f, 1.0f}); // No class label
        
        MarginMeasure measure = new MarginMeasure();
        
        assertThrows(IllegalArgumentException.class, () -> {
            measure.compute(vectorSpace, tempDir, new HashMap<>());
        });
    }

    @Test
    public void testMarginResultMethods() {
        double[] margins = {1.0, Double.NaN, 3.0, Double.POSITIVE_INFINITY};
        VectorUtils.Statistics stats = new VectorUtils.Statistics(2.0, 1.0, 1.0, 3.0);
        MarginMeasure.MarginResult result = new MarginMeasure.MarginResult(margins, stats, 2);
        
        assertEquals(4, result.getVectorCount());
        assertEquals(1.0, result.getMargin(0));
        assertTrue(Double.isNaN(result.getMargin(1)));
        assertEquals(3.0, result.getMargin(2));
        assertTrue(Double.isInfinite(result.getMargin(3)));
        
        assertTrue(result.isValidMargin(0));
        assertFalse(result.isValidMargin(1));
        assertTrue(result.isValidMargin(2));
        assertFalse(result.isValidMargin(3));
        
        assertEquals(2, result.getValidCount());
        assertEquals(0.5, result.getValidFraction(), 1e-10);
        assertEquals(stats, result.getStatistics());
    }

    @Test
    public void testMarginResultToString() {
        MarginMeasure.MarginResult result = new MarginMeasure.MarginResult();
        String str = result.toString();
        
        assertTrue(str.contains("MarginResult"));
        assertTrue(str.contains("vectors=0"));
        assertTrue(str.contains("valid=0"));
    }

    @Test
    public void testMarginWithWellSeparatedClasses() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        
        // Class 0: vectors near origin
        vectorSpace.addVector(new float[]{0.0f, 0.0f}, 0);
        vectorSpace.addVector(new float[]{0.1f, 0.1f}, 0);
        
        // Class 1: vectors far from origin
        vectorSpace.addVector(new float[]{10.0f, 10.0f}, 1);
        vectorSpace.addVector(new float[]{10.1f, 10.1f}, 1);
        
        MarginMeasure measure = new MarginMeasure();
        MarginMeasure.MarginResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        // Well-separated classes should have high margin values
        assertTrue(result.statistics.mean > 1.0, "Well-separated classes should have high margins");
        assertEquals(4, result.validCount);
    }

    @Test
    public void testMarginWithOverlappingClasses() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        
        // Overlapping classes
        vectorSpace.addVector(new float[]{0.0f, 0.0f}, 0);
        vectorSpace.addVector(new float[]{0.1f, 0.1f}, 1); // Very close but different class
        vectorSpace.addVector(new float[]{0.2f, 0.0f}, 0);
        vectorSpace.addVector(new float[]{0.0f, 0.2f}, 1);
        
        MarginMeasure measure = new MarginMeasure();
        MarginMeasure.MarginResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        // Overlapping classes should have lower margin values
        assertTrue(result.statistics.mean < 5.0, "Overlapping classes should have lower margins");
        assertEquals(4, result.validCount);
    }
}