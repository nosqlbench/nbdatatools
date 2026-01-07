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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class AbstractAnalysisMeasureTest {

    @TempDir
    Path tempDir;

    /**
     * Test implementation of AbstractAnalysisMeasure for testing caching behavior.
     */
    private static class TestMeasure extends AbstractAnalysisMeasure<String> {
        private int computeCallCount = 0;
        
        @Override
        public String getMnemonic() {
            return "Test";
        }
        
        @Override
        public String[] getDependencies() {
            return new String[0];
        }
        
        @Override
        protected Class<String> getResultClass() {
            return String.class;
        }
        
        @Override
        protected String computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
            computeCallCount++;
            return "computed-result-" + computeCallCount;
        }
        
        public int getComputeCallCount() {
            return computeCallCount;
        }
    }

    @Test
    public void testCachingBehavior() {
        TestVectorSpace vectorSpace = new TestVectorSpace("cache-test");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        TestMeasure measure = new TestMeasure();
        Map<String, Object> deps = new HashMap<>();
        
        // First computation should call computeImpl
        String result1 = measure.compute(vectorSpace, tempDir, deps);
        assertEquals("computed-result-1", result1);
        assertEquals(1, measure.getComputeCallCount());
        
        // Second computation should use cached result
        String result2 = measure.compute(vectorSpace, tempDir, deps);
        assertEquals("computed-result-1", result2); // Same result
        assertEquals(1, measure.getComputeCallCount()); // No additional computation
    }

    @Test
    public void testCacheFileHandling() throws Exception {
        TestVectorSpace vectorSpace = new TestVectorSpace("file-cache-test");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        TestMeasure measure = new TestMeasure();
        Map<String, Object> deps = new HashMap<>();
        
        // Compute result
        String result = measure.compute(vectorSpace, tempDir, deps);
        assertEquals("computed-result-1", result);
        
        // Check that cache file was created
        String expectedFilename = measure.getCacheFilename(vectorSpace);
        Path cacheFile = tempDir.resolve(expectedFilename);
        assertTrue(Files.exists(cacheFile), "Cache file should be created");
        
        // Create a new measure instance (simulating restart)
        TestMeasure measure2 = new TestMeasure();
        
        // This should load from cache file, not recompute
        String result2 = measure2.compute(vectorSpace, tempDir, deps);
        assertEquals("computed-result-1", result2);
        assertEquals(0, measure2.getComputeCallCount()); // Should not have computed
    }

    @Test
    public void testDefaultCacheFilename() {
        TestVectorSpace vectorSpace = new TestVectorSpace("filename-test");
        TestMeasure measure = new TestMeasure();
        
        String filename = measure.getCacheFilename(vectorSpace);
        
        assertTrue(filename.contains("test")); // From mnemonic
        assertTrue(filename.contains("filename-test")); // From vector space ID
        assertTrue(filename.endsWith(".json"));
    }

    @Test
    public void testInvalidCacheFile() throws Exception {
        TestVectorSpace vectorSpace = new TestVectorSpace("invalid-cache-test");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        TestMeasure measure = new TestMeasure();
        
        // Create an invalid cache file
        String filename = measure.getCacheFilename(vectorSpace);
        Path cacheFile = tempDir.resolve(filename);
        Files.write(cacheFile, "invalid json content".getBytes());
        
        Map<String, Object> deps = new HashMap<>();
        
        // Should fall back to computation when cache file is invalid
        String result = measure.compute(vectorSpace, tempDir, deps);
        assertEquals("computed-result-1", result);
        assertEquals(1, measure.getComputeCallCount());
    }

    @Test
    public void testCacheDirectoryCreation() {
        TestVectorSpace vectorSpace = new TestVectorSpace("dir-test");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        TestMeasure measure = new TestMeasure();
        Path nonExistentDir = tempDir.resolve("non-existent");
        
        assertFalse(Files.exists(nonExistentDir));
        
        Map<String, Object> deps = new HashMap<>();
        String result = measure.compute(vectorSpace, nonExistentDir, deps);
        
        assertEquals("computed-result-1", result);
        // Directory should be created during computation
        assertTrue(Files.exists(nonExistentDir));
    }

    /**
     * Test measure with dependencies to verify dependency handling.
     */
    private static class DependentTestMeasure extends AbstractAnalysisMeasure<String> {
        @Override
        public String getMnemonic() {
            return "Dependent";
        }
        
        @Override
        public String[] getDependencies() {
            return new String[]{"Test"};
        }
        
        @Override
        protected Class<String> getResultClass() {
            return String.class;
        }
        
        @Override
        protected String computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
            String testResult = (String) dependencyResults.get("Test");
            return "dependent-on-" + testResult;
        }
    }

    @Test
    public void testMeasureWithDependencies() {
        TestVectorSpace vectorSpace = new TestVectorSpace("dep-test");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        DependentTestMeasure measure = new DependentTestMeasure();
        Map<String, Object> deps = new HashMap<>();
        deps.put("Test", "dependency-value");
        
        String result = measure.compute(vectorSpace, tempDir, deps);
        assertEquals("dependent-on-dependency-value", result);
    }
}