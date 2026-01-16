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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class VectorSpaceAnalysisUtilsTest {

    @TempDir
    Path tempDir;

    @Test
    public void testAnalyzeAndReport() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        String report = VectorSpaceAnalysisUtils.analyzeAndReport(vectorSpace);
        
        assertNotNull(report);
        assertFalse(report.isEmpty());
        assertTrue(report.contains("Vector Space Analysis Report"));
        assertTrue(report.contains(vectorSpace.getId()));
    }

    @Test
    public void testAnalyzeAndSaveReport() throws IOException {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        Path outputFile = tempDir.resolve("analysis-report.txt");
        
        VectorSpaceAnalysisUtils.analyzeAndSaveReport(vectorSpace, outputFile);
        
        assertTrue(Files.exists(outputFile));
        String content = Files.readString(outputFile);
        assertFalse(content.isEmpty());
        assertTrue(content.contains("Vector Space Analysis Report"));
    }

    @Test
    public void testAnalyzeWithCache() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        
        VectorSpaceAnalyzer.AnalysisReport report = VectorSpaceAnalysisUtils.analyzeWithCache(vectorSpace, tempDir);
        
        assertNotNull(report);
        assertEquals(vectorSpace, report.vectorSpace);
        assertFalse(report.results.isEmpty());
    }

    @Test
    public void testToCsvReport() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        String csv = VectorSpaceAnalysisUtils.toCsvReport(report);
        
        assertNotNull(csv);
        assertFalse(csv.isEmpty());
        
        // Check CSV format
        String[] lines = csv.split("\n");
        assertTrue(lines.length > 1);
        assertEquals("Metric,Value", lines[0]); // Header
        
        // Check that some expected metrics are present
        assertTrue(csv.contains("VectorSpaceId"));
        assertTrue(csv.contains("VectorCount"));
        assertTrue(csv.contains("Dimension"));
        assertTrue(csv.contains("LID_Mean"));
        assertTrue(csv.contains("Hubness_Skewness"));
    }

    @Test
    public void testToJsonReport() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        String json = VectorSpaceAnalysisUtils.toJsonReport(report);
        
        assertNotNull(json);
        assertFalse(json.isEmpty());
        
        // Check JSON structure
        assertTrue(json.startsWith("{"));
        assertTrue(json.trim().endsWith("}"));
        assertTrue(json.contains("\"vectorSpaceId\""));
        assertTrue(json.contains("\"vectorCount\""));
        assertTrue(json.contains("\"results\""));
        assertTrue(json.contains("\"LID\""));
        assertTrue(json.contains("\"Hubness\""));
    }

    @Test
    public void testInterpretResults() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        String interpretation = VectorSpaceAnalysisUtils.interpretResults(report);
        
        assertNotNull(interpretation);
        assertFalse(interpretation.isEmpty());
        assertTrue(interpretation.contains("Vector Space Analysis Interpretation"));
        assertTrue(interpretation.contains("Local Intrinsic Dimensionality"));
        assertTrue(interpretation.contains("Class Separability"));
        assertTrue(interpretation.contains("Hubness Analysis"));
        assertTrue(interpretation.contains(vectorSpace.getId()));
    }

    @Test
    public void testToCsvReportWithMissingResults() {
        // Create a minimal report with only basic vector space info
        TestVectorSpace vectorSpace = new TestVectorSpace("minimal-space");
        vectorSpace.addVector(new float[]{1.0f, 2.0f});
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        
        // This should fail because of missing class labels for Margin measure
        assertThrows(IllegalArgumentException.class, () -> {
            analyzer.analyzeVectorSpace(vectorSpace);
        });
    }

    @Test
    public void testJsonReportFormatting() {
        TestVectorSpace vectorSpace = TestVectorSpace.createTestSpace();
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(tempDir);
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        String json = VectorSpaceAnalysisUtils.toJsonReport(report);
        
        // Basic JSON validation - should be parseable
        // Check for properly formatted numeric values
        assertTrue(json.contains("\"mean\": "));
        assertTrue(json.contains("\"stdDev\": "));
        
        // Check nested structure
        assertTrue(json.contains("\"inDegreeStats\": {"));
        
        // Ensure no trailing commas in JSON
        assertFalse(json.contains(",\n  }"));
        assertFalse(json.contains(",\n}"));
    }
}