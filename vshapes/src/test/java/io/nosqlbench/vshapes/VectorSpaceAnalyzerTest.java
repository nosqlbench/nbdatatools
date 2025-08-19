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

import io.nosqlbench.vshapes.measures.HubnessMeasure;
import io.nosqlbench.vshapes.measures.LIDMeasure;
import io.nosqlbench.vshapes.measures.MarginMeasure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class VectorSpaceAnalyzerTest {

    @TempDir
    Path tempDir;

    @Test
    public void testAnalyzerCreation() {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        assertEquals(tempDir, analyzer.getCacheDirectory());
    }

    @Test
    public void testAnalyzerDefaultCreation() {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        assertNotNull(analyzer.getCacheDirectory());
        assertTrue(analyzer.getCacheDirectory().toString().contains("vshapes-cache"));
    }

    @Test
    public void testAnalyzeVectorSpace() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        assertNotNull(report);
        assertEquals(vectorSpace, report.vectorSpace);
        assertFalse(report.results.isEmpty());
        
        // Check that default measures were computed
        assertTrue(report.results.containsKey("LID"));
        assertTrue(report.results.containsKey("Margin"));
        assertTrue(report.results.containsKey("Hubness"));
    }

    @Test
    public void testGetSpecificResults() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        analyzer.analyzeVectorSpace(vectorSpace);
        
        LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();
        assertNotNull(lidResult);
        
        MarginMeasure.MarginResult marginResult = analyzer.getMarginResult();
        assertNotNull(marginResult);
        
        HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();
        assertNotNull(hubnessResult);
    }

    @Test
    public void testCustomMeasureRegistration() {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        // Create a custom measure
        LIDMeasure customLID = new LIDMeasure(5) {
            @Override
            public String getMnemonic() {
                return "CustomLID";
            }
        };
        
        analyzer.registerMeasure(customLID);
        
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        assertTrue(report.results.containsKey("CustomLID"));
    }

    @Test
    public void testAnalysisReportSummary() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        String summary = report.getSummary();
        
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
        
        // Check that summary contains expected sections
        assertTrue(summary.contains("Vector Space Analysis Report"));
        assertTrue(summary.contains("Local Intrinsic Dimensionality"));
        assertTrue(summary.contains("Nearest-Neighbor Margin"));
        assertTrue(summary.contains("Hubness Analysis"));
        assertTrue(summary.contains(vectorSpace.getId()));
    }

    @Test
    public void testAnalysisReportToString() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        String str = report.toString();
        
        assertEquals(report.getSummary(), str);
    }

    @Test
    public void testAnalysisReportGetResult() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
        assertNotNull(lidResult);
        
        // Test with wrong class type
        String wrongType = report.getResult("LID", String.class);
        assertNull(wrongType);
        
        // Test with non-existent measure
        Object nonExistent = report.getResult("NonExistent", Object.class);
        assertNull(nonExistent);
    }

    @Test
    public void testClearCache() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        // First analysis
        analyzer.analyzeVectorSpace(vectorSpace);
        assertNotNull(analyzer.getLIDResult());
        
        // Clear cache
        analyzer.clearCache();
        
        // Results should be cleared from memory
        assertNull(analyzer.getLIDResult());
    }

    @Test
    public void testAnalyzerWithVectorSpaceWithoutClassLabels() {
        TestVectorSpace vectorSpace = new TestVectorSpace();
        vectorSpace.addVector(new float[]{0.0f, 0.0f}); // No class label
        vectorSpace.addVector(new float[]{1.0f, 1.0f}); // No class label
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        // This should throw an exception because MarginMeasure requires class labels
        assertThrows(IllegalArgumentException.class, () -> {
            analyzer.analyzeVectorSpace(vectorSpace);
        });
    }

    @Test
    public void testGetResultWithGenericMethod() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        analyzer.analyzeVectorSpace(vectorSpace);
        
        // Test generic getResult method
        LIDMeasure.LIDResult lidResult = analyzer.getResult("LID", LIDMeasure.LIDResult.class);
        assertNotNull(lidResult);
        
        // Test with wrong type
        String wrongResult = analyzer.getResult("LID", String.class);
        assertNull(wrongResult);
        
        // Test with non-existent measure
        Object nonExistent = analyzer.getResult("NonExistent", Object.class);
        assertNull(nonExistent);
    }

    @Test
    public void testAnalysisWithLargerDataset() {
        TestVectorSpace vectorSpace = TestVectorSpace.createHighDimTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        assertNotNull(report);
        
        // Verify all measures completed successfully
        assertTrue(report.results.containsKey("LID"));
        assertTrue(report.results.containsKey("Margin"));
        assertTrue(report.results.containsKey("Hubness"));
        
        // Check that summary is comprehensive
        String summary = report.getSummary();
        assertTrue(summary.contains("Vectors: 20"));
        assertTrue(summary.contains("Dimensions: 10"));
        assertTrue(summary.contains("Has Class Labels: true"));
    }
}