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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/// # VectorSpaceAnalyzer
///
/// Main orchestrator for vector space analysis operations.
/// 
/// ## Purpose
/// This class coordinates the execution of various analysis measures,
/// handles dependency resolution between measures, and manages caching 
/// of computational artifacts for performance optimization.
///
/// ## Features
/// - **Measure Registration**: Support for pluggable analysis measures
/// - **Dependency Management**: Automatic resolution of measure dependencies
/// - **Caching**: Persistent caching of expensive computations
/// - **Comprehensive Reporting**: Unified analysis reports with multiple measures
///
/// ## Usage
/// ```java
/// VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
/// AnalysisReport report = analyzer.analyzeVectorSpace(myVectorSpace);
/// System.out.println(report.getSummary());
/// ```
///
/// ## Default Measures
/// The analyzer comes pre-configured with:
/// - **LID**: Local Intrinsic Dimensionality analysis
/// - **Margin**: Nearest-neighbor margin analysis  
/// - **Hubness**: Hub and anti-hub detection
public class VectorSpaceAnalyzer {

    private final Path cacheDir;
    private final Map<String, AnalysisMeasure<?>> measures;
    private final Map<String, Object> computedResults;

    /// Creates a new analyzer with the specified cache directory.
    /// 
    /// @param cacheDir directory for storing computational artifacts
    public VectorSpaceAnalyzer(Path cacheDir) {
        this.cacheDir = cacheDir;
        this.measures = new LinkedHashMap<>();
        this.computedResults = new HashMap<>();
        
        // Register default measures
        registerMeasure(new LIDMeasure());
        registerMeasure(new MarginMeasure());
        registerMeasure(new HubnessMeasure());
    }

    /// Creates a new analyzer with default cache directory in temp space.
    /// Uses system temporary directory with a "vshapes-cache" subdirectory.
    public VectorSpaceAnalyzer() {
        this(Paths.get(System.getProperty("java.io.tmpdir"), "vshapes-cache"));
    }

    /// Registers a new analysis measure with the analyzer.
    /// 
    /// @param measure the measure to register
    public void registerMeasure(AnalysisMeasure<?> measure) {
        measures.put(measure.getMnemonic(), measure);
    }

    /// Performs comprehensive analysis of a vector space.
    /// Executes all registered measures and returns a unified report.
    /// 
    /// @param vectorSpace the vector space to analyze
    /// @return analysis report with all computed measures
    public AnalysisReport analyzeVectorSpace(VectorSpace vectorSpace) {
        computedResults.clear();
        
        // Ensure cache directory exists
        try {
            Files.createDirectories(cacheDir);
        } catch (Exception e) {
            System.err.println("Warning: Failed to create cache directory: " + e.getMessage());
        }

        // Compute all registered measures
        for (AnalysisMeasure<?> measure : measures.values()) {
            computeMeasure(measure, vectorSpace);
        }

        return new AnalysisReport(vectorSpace, new HashMap<>(computedResults));
    }

    /// Computes a specific measure, handling dependencies automatically.
    /// 
    /// @param measure the measure to compute
    /// @param vectorSpace the vector space
    /// @return the computed result
    private Object computeMeasure(AnalysisMeasure<?> measure, VectorSpace vectorSpace) {
        String mnemonic = measure.getMnemonic();
        
        // Check if already computed
        if (computedResults.containsKey(mnemonic)) {
            return computedResults.get(mnemonic);
        }

        // Compute dependencies first
        Map<String, Object> dependencyResults = new HashMap<>();
        for (String dependency : measure.getDependencies()) {
            AnalysisMeasure<?> depMeasure = measures.get(dependency);
            if (depMeasure == null) {
                throw new IllegalStateException("Unknown dependency: " + dependency + " for measure: " + mnemonic);
            }
            Object depResult = computeMeasure(depMeasure, vectorSpace);
            dependencyResults.put(dependency, depResult);
        }

        // Compute this measure
        Object result = measure.compute(vectorSpace, cacheDir, dependencyResults);
        computedResults.put(mnemonic, result);

        return result;
    }

    /// Gets a specific computed measure result by mnemonic.
    /// 
    /// @param mnemonic the measure mnemonic (e.g., "LID", "Margin", "Hubness")
    /// @param resultClass the expected result class for type safety
    /// @param <T> the result type
    /// @return the computed result, or null if not computed or type mismatch
    @SuppressWarnings("unchecked")
    public <T> T getResult(String mnemonic, Class<T> resultClass) {
        Object result = computedResults.get(mnemonic);
        if (result != null && resultClass.isAssignableFrom(result.getClass())) {
            return (T) result;
        }
        return null;
    }

    /// Gets the LID (Local Intrinsic Dimensionality) analysis result.
    /// 
    /// @return LID result with per-vector values and statistics, or null if not computed
    public LIDMeasure.LIDResult getLIDResult() {
        return getResult("LID", LIDMeasure.LIDResult.class);
    }

    /// Gets the Margin analysis result.
    /// 
    /// @return Margin result with class separability metrics, or null if not computed
    public MarginMeasure.MarginResult getMarginResult() {
        return getResult("Margin", MarginMeasure.MarginResult.class);
    }

    /// Gets the Hubness analysis result.
    /// 
    /// @return Hubness result with hub/anti-hub detection, or null if not computed
    public HubnessMeasure.HubnessResult getHubnessResult() {
        return getResult("Hubness", HubnessMeasure.HubnessResult.class);
    }

    /// Gets the cache directory being used for computational artifacts.
    /// 
    /// @return cache directory path
    public Path getCacheDirectory() {
        return cacheDir;
    }

    /// Clears the computation cache, forcing recomputation on next analysis.
    /// This removes both in-memory results and cached files on disk.
    public void clearCache() {
        computedResults.clear();
        try {
            if (Files.exists(cacheDir)) {
                Files.walk(cacheDir)
                     .filter(Files::isRegularFile)
                     .forEach(path -> {
                         try {
                             Files.delete(path);
                         } catch (Exception e) {
                             System.err.println("Warning: Failed to delete cache file: " + path);
                         }
                     });
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to clear cache directory: " + e.getMessage());
        }
    }

    /// ## AnalysisReport
    ///
    /// Comprehensive analysis report containing all computed measures for a vector space.
    /// 
    /// ### Features
    /// - **Unified Results**: All measure results in a single report
    /// - **Text Summaries**: Human-readable analysis summaries
    /// - **Type-Safe Access**: Strongly-typed result retrieval
    /// 
    /// ### Usage
    /// ```java
    /// AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
    /// LIDMeasure.LIDResult lid = report.getResult("LID", LIDMeasure.LIDResult.class);
    /// System.out.println(report.getSummary());
    /// ```
    public static class AnalysisReport {
        /// The vector space that was analyzed
        public final VectorSpace vectorSpace;
        /// Map of measure mnemonics to their computed results
        public final Map<String, Object> results;

        /// Creates a new analysis report.
        /// 
        /// @param vectorSpace the analyzed vector space
        /// @param results map of measure mnemonics to their computed results
        public AnalysisReport(VectorSpace vectorSpace, Map<String, Object> results) {
            this.vectorSpace = vectorSpace;
            this.results = results;
        }

        /// Gets a specific result by measure mnemonic with type safety.
        /// 
        /// @param mnemonic the measure mnemonic (e.g., "LID", "Margin", "Hubness")
        /// @param resultClass the expected result class for type checking
        /// @param <T> the result type
        /// @return the result, or null if not available or type mismatch
        @SuppressWarnings("unchecked")
        public <T> T getResult(String mnemonic, Class<T> resultClass) {
            Object result = results.get(mnemonic);
            if (result != null && resultClass.isAssignableFrom(result.getClass())) {
                return (T) result;
            }
            return null;
        }

        /// Generates a comprehensive text summary of the analysis results.
        /// 
        /// @return formatted multi-line summary string with all computed measures
        public String getSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Vector Space Analysis Report\n");
            sb.append("===========================\n");
            sb.append(String.format("Vector Space: %s\n", vectorSpace.getId()));
            sb.append(String.format("Vectors: %d, Dimensions: %d\n", 
                                  vectorSpace.getVectorCount(), vectorSpace.getDimension()));
            sb.append(String.format("Has Class Labels: %s\n", vectorSpace.hasClassLabels()));
            sb.append("\n");

            // LID Summary
            LIDMeasure.LIDResult lidResult = getResult("LID", LIDMeasure.LIDResult.class);
            if (lidResult != null) {
                sb.append("Local Intrinsic Dimensionality (LID):\n");
                sb.append(String.format("  Mean: %.2f ± %.2f (std dev)\n", 
                                      lidResult.statistics.mean, lidResult.statistics.stdDev));
                sb.append(String.format("  Range: %.2f to %.2f\n", 
                                      lidResult.statistics.min, lidResult.statistics.max));
                sb.append("\n");
            }

            // Margin Summary
            MarginMeasure.MarginResult marginResult = getResult("Margin", MarginMeasure.MarginResult.class);
            if (marginResult != null) {
                sb.append("Nearest-Neighbor Margin:\n");
                sb.append(String.format("  Mean: %.3f ± %.3f (std dev)\n", 
                                      marginResult.statistics.mean, marginResult.statistics.stdDev));
                sb.append(String.format("  Range: %.3f to %.3f\n", 
                                      marginResult.statistics.min, marginResult.statistics.max));
                sb.append(String.format("  Valid Margins: %d/%d (%.1f%%)\n",
                                      marginResult.validCount, marginResult.getVectorCount(),
                                      marginResult.getValidFraction() * 100));
                sb.append("\n");
            }

            // Hubness Summary
            HubnessMeasure.HubnessResult hubnessResult = getResult("Hubness", HubnessMeasure.HubnessResult.class);
            if (hubnessResult != null) {
                sb.append("Hubness Analysis:\n");
                sb.append(String.format("  Skewness: %.3f\n", hubnessResult.skewness));
                sb.append(String.format("  Hubs: %d (%.1f%%)\n", 
                                      hubnessResult.hubCount, hubnessResult.getHubFraction() * 100));
                sb.append(String.format("  Anti-hubs: %d (%.1f%%)\n", 
                                      hubnessResult.antiHubCount, hubnessResult.getAntiHubFraction() * 100));
                sb.append(String.format("  In-degree mean: %.1f ± %.1f\n",
                                      hubnessResult.inDegreeStats.mean, hubnessResult.inDegreeStats.stdDev));
                sb.append("\n");
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            return getSummary();
        }
    }
}