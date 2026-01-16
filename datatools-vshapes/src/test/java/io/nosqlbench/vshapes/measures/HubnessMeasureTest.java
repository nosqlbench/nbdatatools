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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class HubnessMeasureTest {

    @TempDir
    Path tempDir;

    @Test
    public void testHubnessMeasureBasic() {
        HubnessMeasure measure = new HubnessMeasure(5);
        assertEquals("Hubness", measure.getMnemonic());
        assertEquals(0, measure.getDependencies().length);
        assertEquals(HubnessMeasure.HubnessResult.class, measure.getResultClass());
    }

    @Test
    public void testHubnessComputation() {
        TestVectorSpace vectorSpace = TestVectorSpace.createHighDimTestSpace();
        HubnessMeasure measure = new HubnessMeasure(3);
        
        HubnessMeasure.HubnessResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertNotNull(result);
        assertEquals(vectorSpace.getVectorCount(), result.getVectorCount());
        assertEquals(3, result.k);
        
        // Check in-degrees are non-negative
        for (int i = 0; i < result.inDegrees.length; i++) {
            assertTrue(result.getInDegree(i) >= 0, "In-degrees should be non-negative");
        }
        
        // Check statistics
        assertNotNull(result.inDegreeStats);
        assertNotNull(result.hubnessStats);
        
        // Check fractions are valid
        assertTrue(result.getHubFraction() >= 0.0 && result.getHubFraction() <= 1.0);
        assertTrue(result.getAntiHubFraction() >= 0.0 && result.getAntiHubFraction() <= 1.0);
    }

    @Test
    public void testHubnessResultMethods() {
        int[] inDegrees = {0, 5, 10, 2, 8};
        double[] hubnessScores = {-2.5, 0.0, 2.5, -0.5, 1.5};
        VectorUtils.Statistics inDegreeStats = VectorUtils.computeStatistics(new double[]{0, 5, 10, 2, 8});
        VectorUtils.Statistics hubnessStats = VectorUtils.computeStatistics(hubnessScores);
        
        HubnessMeasure.HubnessResult result = new HubnessMeasure.HubnessResult(
            inDegrees, hubnessScores, inDegreeStats, hubnessStats, 0.5, 1, 1, 3
        );
        
        assertEquals(5, result.getVectorCount());
        assertEquals(0, result.getInDegree(0));
        assertEquals(5, result.getInDegree(1));
        assertEquals(10, result.getInDegree(2));
        
        assertEquals(-2.5, result.getHubnessScore(0));
        assertEquals(0.0, result.getHubnessScore(1));
        assertEquals(2.5, result.getHubnessScore(2));
        
        assertFalse(result.isHub(0));
        assertFalse(result.isHub(1));
        assertTrue(result.isHub(2)); // hubness score > 2.0
        
        assertTrue(result.isAntiHub(0)); // hubness score < -2.0
        assertFalse(result.isAntiHub(1));
        assertFalse(result.isAntiHub(2));
        
        assertEquals(0.5, result.getSkewness());
        assertEquals(1, result.hubCount);
        assertEquals(1, result.antiHubCount);
        assertEquals(0.2, result.getHubFraction(), 1e-10); // 1/5
        assertEquals(0.2, result.getAntiHubFraction(), 1e-10); // 1/5
    }

    @Test
    public void testHubnessWithUniformDistribution() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        
        // Create a uniform grid of points
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                vectorSpace.addVector(new float[]{i * 2.0f, j * 2.0f});
            }
        }
        
        HubnessMeasure measure = new HubnessMeasure(3);
        HubnessMeasure.HubnessResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        // Uniform distribution should have low skewness
        assertTrue(Math.abs(result.skewness) < 2.0, "Uniform distribution should have low skewness");
        
        // Should have few or no extreme hubs/anti-hubs
        assertTrue(result.getHubFraction() < 0.3);
        assertTrue(result.getAntiHubFraction() < 0.3);
    }

    @Test
    public void testHubnessResultToString() {
        HubnessMeasure.HubnessResult result = new HubnessMeasure.HubnessResult();
        String str = result.toString();
        
        assertTrue(str.contains("HubnessResult"));
        assertTrue(str.contains("k=0"));
        assertTrue(str.contains("vectors=0"));
        assertTrue(str.contains("skewness="));
    }

    @Test
    public void testCacheFilename() {
        TestVectorSpace vectorSpace = new TestVectorSpace("hub-test");
        HubnessMeasure measure = new HubnessMeasure(5);
        
        String filename = measure.getCacheFilename(vectorSpace);
        
        assertTrue(filename.contains("hubness"));
        assertTrue(filename.contains("k5"));
        assertTrue(filename.contains("hub-test"));
        assertTrue(filename.endsWith(".json"));
    }

    @Test
    public void testDefaultK() {
        HubnessMeasure measure = new HubnessMeasure();
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        
        HubnessMeasure.HubnessResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        assertEquals(10, result.k); // Default k should be 10
    }

    @Test
    public void testSkewnessComputation() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        
        // Create a hub scenario: one central point with many points around it
        vectorSpace.addVector(new float[]{0.0f, 0.0f}); // Central hub
        
        // Add points in a circle around the hub
        for (int i = 0; i < 10; i++) {
            double angle = 2 * Math.PI * i / 10;
            vectorSpace.addVector(new float[]{(float) Math.cos(angle), (float) Math.sin(angle)});
        }
        
        HubnessMeasure measure = new HubnessMeasure(3);
        HubnessMeasure.HubnessResult result = measure.compute(vectorSpace, tempDir, new HashMap<>());
        
        // The central point should have high in-degree, creating positive skewness
        assertTrue(result.skewness > 0, "Hub scenario should create positive skewness");
    }
}