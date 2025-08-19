/*
 * Basic usage example for Vector Space Analysis
 * 
 * This example demonstrates how to:
 * 1. Create a simple vector space
 * 2. Perform analysis
 * 3. Display results in different formats
 */

package examples;

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


import io.nosqlbench.vshapes.*;
import io.nosqlbench.vshapes.measures.*;
import java.nio.file.Paths;
import java.util.Optional;

public class BasicUsageExample {
    
    public static void main(String[] args) {
        // Create sample data: two well-separated clusters
        VectorSpace vectorSpace = createSampleData();
        
        // Perform analysis
        performBasicAnalysis(vectorSpace);
        
        // Try different output formats
        demonstrateOutputFormats(vectorSpace);
    }
    
    private static VectorSpace createSampleData() {
        // Create two clusters in 3D space
        float[][] vectors = {
            // Cluster 1 (class 0) - around origin
            {0.0f, 0.0f, 0.0f}, {0.1f, 0.1f, 0.1f}, {-0.1f, 0.1f, 0.0f},
            {0.2f, 0.0f, 0.1f}, {0.0f, -0.1f, 0.1f}, {0.1f, 0.0f, -0.1f},
            
            // Cluster 2 (class 1) - displaced
            {3.0f, 3.0f, 3.0f}, {3.1f, 3.2f, 2.9f}, {2.9f, 3.1f, 3.1f},
            {3.2f, 2.8f, 3.0f}, {2.8f, 3.0f, 3.2f}, {3.1f, 2.9f, 2.8f}
        };
        
        int[] labels = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1};
        
        return new SimpleVectorSpace("example-clusters", vectors, labels);
    }
    
    private static void performBasicAnalysis(VectorSpace vectorSpace) {
        System.out.println("=== BASIC VECTOR SPACE ANALYSIS ===\n");
        
        // Create analyzer
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        
        // Perform analysis
        System.out.println("Analyzing " + vectorSpace.getVectorCount() + " vectors...");
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        // Display summary
        System.out.println(report.getSummary());
        
        // Access specific results
        LIDMeasure.LIDResult lidResult = analyzer.getLIDResult();
        if (lidResult != null) {
            System.out.printf("\nDetailed LID Information:\n");
            System.out.printf("- k parameter used: %d\n", lidResult.k);
            System.out.printf("- Individual LID values:\n");
            for (int i = 0; i < Math.min(5, lidResult.getVectorCount()); i++) {
                System.out.printf("  Vector %d: %.2f\n", i, lidResult.getLID(i));
            }
        }
        
        HubnessMeasure.HubnessResult hubnessResult = analyzer.getHubnessResult();
        if (hubnessResult != null) {
            System.out.printf("\nHub Detection:\n");
            for (int i = 0; i < hubnessResult.getVectorCount(); i++) {
                if (hubnessResult.isHub(i)) {
                    System.out.printf("- Vector %d is a hub (score: %.2f)\n", 
                                     i, hubnessResult.getHubnessScore(i));
                }
                if (hubnessResult.isAntiHub(i)) {
                    System.out.printf("- Vector %d is an anti-hub (score: %.2f)\n", 
                                     i, hubnessResult.getHubnessScore(i));
                }
            }
        }
    }
    
    private static void demonstrateOutputFormats(VectorSpace vectorSpace) {
        System.out.println("\n=== OUTPUT FORMATS DEMONSTRATION ===\n");
        
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        
        // CSV format
        System.out.println("CSV Format:");
        System.out.println("----------");
        String csvReport = VectorSpaceAnalysisUtils.toCsvReport(report);
        System.out.println(csvReport);
        
        // JSON format (first few lines)
        System.out.println("\nJSON Format (excerpt):");
        System.out.println("---------------------");
        String jsonReport = VectorSpaceAnalysisUtils.toJsonReport(report);
        String[] jsonLines = jsonReport.split("\n");
        for (int i = 0; i < Math.min(10, jsonLines.length); i++) {
            System.out.println(jsonLines[i]);
        }
        if (jsonLines.length > 10) {
            System.out.println("... (truncated)");
        }
        
        // Interpretation
        System.out.println("\nInterpretation:");
        System.out.println("--------------");
        String interpretation = VectorSpaceAnalysisUtils.interpretResults(report);
        System.out.println(interpretation);
    }
    
    /**
     * Simple VectorSpace implementation for examples
     */
    public static class SimpleVectorSpace implements VectorSpace {
        private final String id;
        private final float[][] vectors;
        private final int[] classLabels;
        
        public SimpleVectorSpace(String id, float[][] vectors, int[] classLabels) {
            this.id = id;
            this.vectors = vectors.clone();
            this.classLabels = classLabels.clone();
        }
        
        @Override
        public String getId() { return id; }
        
        @Override
        public int getVectorCount() { return vectors.length; }
        
        @Override
        public int getDimension() { return vectors[0].length; }
        
        @Override
        public float[] getVector(int index) { return vectors[index].clone(); }
        
        @Override
        public float[][] getAllVectors() { return vectors.clone(); }
        
        @Override
        public Optional<Integer> getClassLabel(int index) {
            return Optional.of(classLabels[index]);
        }
    }
}
