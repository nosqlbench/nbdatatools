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

package io.nosqlbench.vshapes;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class VectorUtilsTest {

    @Test
    public void testEuclideanDistance() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 6.0f, 8.0f};
        
        double expected = Math.sqrt(9 + 16 + 25); // sqrt(50) = ~7.07
        double actual = VectorUtils.euclideanDistance(a, b);
        
        assertEquals(expected, actual, 1e-10);
    }

    @Test
    public void testEuclideanDistanceSame() {
        float[] a = {1.0f, 2.0f, 3.0f};
        
        assertEquals(0.0, VectorUtils.euclideanDistance(a, a), 1e-10);
    }

    @Test
    public void testEuclideanDistanceDifferentDimensions() {
        float[] a = {1.0f, 2.0f};
        float[] b = {1.0f, 2.0f, 3.0f};
        
        assertThrows(IllegalArgumentException.class, () -> {
            VectorUtils.euclideanDistance(a, b);
        });
    }

    @Test
    public void testSquaredEuclideanDistance() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 6.0f, 8.0f};
        
        double expected = 9 + 16 + 25; // 50
        double actual = VectorUtils.squaredEuclideanDistance(a, b);
        
        assertEquals(expected, actual, 1e-10);
    }

    @Test
    public void testFindKNearestNeighbors() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        vectorSpace.addVector(new float[]{0.0f, 0.0f}); // index 0
        vectorSpace.addVector(new float[]{1.0f, 0.0f}); // index 1
        vectorSpace.addVector(new float[]{0.0f, 1.0f}); // index 2
        vectorSpace.addVector(new float[]{2.0f, 2.0f}); // index 3
        
        float[] query = {0.0f, 0.0f};
        int[] neighbors = VectorUtils.findKNearestNeighbors(query, vectorSpace, 2, 0);
        
        assertEquals(2, neighbors.length);
        // Should be indices 1 and 2 (closest to origin after excluding index 0)
        assertTrue((neighbors[0] == 1 && neighbors[1] == 2) || 
                   (neighbors[0] == 2 && neighbors[1] == 1));
    }

    @Test
    public void testComputeAllKNN() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        vectorSpace.addVector(new float[]{0.0f, 0.0f}); // index 0
        vectorSpace.addVector(new float[]{1.0f, 0.0f}); // index 1
        vectorSpace.addVector(new float[]{0.0f, 1.0f}); // index 2
        
        int[][] knn = VectorUtils.computeAllKNN(vectorSpace, 1);
        
        assertEquals(3, knn.length);
        assertEquals(1, knn[0].length);
        assertEquals(1, knn[1].length);
        assertEquals(1, knn[2].length);
    }

    @Test
    public void testComputeStatistics() {
        double[] values = {1.0, 2.0, 3.0, 4.0, 5.0};
        
        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);
        
        assertEquals(3.0, stats.mean, 1e-10);
        assertEquals(1.0, stats.min, 1e-10);
        assertEquals(5.0, stats.max, 1e-10);
        assertTrue(stats.stdDev > 0);
    }

    @Test
    public void testComputeStatisticsEmpty() {
        double[] values = {};
        
        VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);
        
        assertEquals(0.0, stats.mean, 1e-10);
        assertEquals(0.0, stats.stdDev, 1e-10);
        assertEquals(0.0, stats.min, 1e-10);
        assertEquals(0.0, stats.max, 1e-10);
    }

    @Test
    public void testStatisticsToString() {
        VectorUtils.Statistics stats = new VectorUtils.Statistics(1.0, 0.5, 0.0, 2.0);
        String str = stats.toString();
        
        assertTrue(str.contains("mean=1.0000"));
        assertTrue(str.contains("stdDev=0.5000"));
        assertTrue(str.contains("min=0.0000"));
        assertTrue(str.contains("max=2.0000"));
    }
}