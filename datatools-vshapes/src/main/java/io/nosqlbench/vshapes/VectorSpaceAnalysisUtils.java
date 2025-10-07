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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/// # VectorSpaceAnalysisUtils
///
/// Utility methods for vector space analysis and reporting in various formats.
///
/// ## Purpose
/// Provides convenient static methods for:
/// - **Quick Analysis**: One-line analysis with default settings
/// - **File I/O**: Saving reports to files
/// - **Format Conversion**: CSV, JSON, and interpretation outputs
/// - **Batch Processing**: Analysis with custom caching
///
/// ## Usage Examples
///
/// ### Quick Analysis
/// ```java
/// String summary = VectorSpaceAnalysisUtils.analyzeAndReport(myVectorSpace);
/// System.out.println(summary);
/// ```
///
/// ### Save to File
/// ```java
/// VectorSpaceAnalysisUtils.analyzeAndSaveReport(vectorSpace, Paths.get("analysis.txt"));
/// ```
///
/// ### Custom Formats
/// ```java
/// VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
/// AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
/// String csv = VectorSpaceAnalysisUtils.toCsvReport(report);
/// String json = VectorSpaceAnalysisUtils.toJsonReport(report);
/// ```
public final class VectorSpaceAnalysisUtils {

    private VectorSpaceAnalysisUtils() {} // Utility class

    /// Analyzes a vector space and returns a formatted text report.
    /// Uses default analyzer with temporary cache directory.
    /// 
    /// @param vectorSpace the vector space to analyze
    /// @return formatted multi-line analysis report
    public static String analyzeAndReport(VectorSpace vectorSpace) {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer();
        VectorSpaceAnalyzer.AnalysisReport report = analyzer.analyzeVectorSpace(vectorSpace);
        return report.getSummary();
    }

    /// Analyzes a vector space and saves the text report to a file.
    /// 
    /// @param vectorSpace the vector space to analyze
    /// @param outputPath path to save the report
    /// @throws IOException if writing fails
    public static void analyzeAndSaveReport(VectorSpace vectorSpace, Path outputPath) throws IOException {
        String report = analyzeAndReport(vectorSpace);
        Files.writeString(outputPath, report);
    }

    /// Analyzes a vector space with a custom cache directory for performance.
    /// Useful for batch processing where caching can speed up repeated operations.
    /// 
    /// @param vectorSpace the vector space to analyze
    /// @param cacheDir directory for caching computational artifacts
    /// @return analysis report with all computed measures
    public static VectorSpaceAnalyzer.AnalysisReport analyzeWithCache(VectorSpace vectorSpace, Path cacheDir) {
        VectorSpaceAnalyzer analyzer = new VectorSpaceAnalyzer(cacheDir);
        return analyzer.analyzeVectorSpace(vectorSpace);
    }

    /// Creates a CSV report of analysis results suitable for spreadsheet import.
    /// Includes all computed metrics in a structured key-value format.
    /// 
    /// @param report the analysis report
    /// @return CSV formatted string with headers
    public static String toCsvReport(VectorSpaceAnalyzer.AnalysisReport report) {
        StringBuilder csv = new StringBuilder();
        
        // Header
        csv.append("Metric,Value\n");
        
        // Basic vector space info
        csv.append(String.format("VectorSpaceId,%s\n", report.vectorSpace.getId()));
        csv.append(String.format("VectorCount,%d\n", report.vectorSpace.getVectorCount()));
        csv.append(String.format("Dimension,%d\n", report.vectorSpace.getDimension()));
        csv.append(String.format("HasClassLabels,%s\n", report.vectorSpace.hasClassLabels()));
        
        // LID results
        LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
        if (lidResult != null) {
            csv.append(String.format("LID_Mean,%.4f\n", lidResult.statistics.mean));
            csv.append(String.format("LID_StdDev,%.4f\n", lidResult.statistics.stdDev));
            csv.append(String.format("LID_Min,%.4f\n", lidResult.statistics.min));
            csv.append(String.format("LID_Max,%.4f\n", lidResult.statistics.max));
        }
        
        // Margin results
        MarginMeasure.MarginResult marginResult = report.getResult("Margin", MarginMeasure.MarginResult.class);
        if (marginResult != null) {
            csv.append(String.format("Margin_Mean,%.4f\n", marginResult.statistics.mean));
            csv.append(String.format("Margin_StdDev,%.4f\n", marginResult.statistics.stdDev));
            csv.append(String.format("Margin_Min,%.4f\n", marginResult.statistics.min));
            csv.append(String.format("Margin_Max,%.4f\n", marginResult.statistics.max));
            csv.append(String.format("Margin_ValidCount,%d\n", marginResult.validCount));
            csv.append(String.format("Margin_ValidFraction,%.4f\n", marginResult.getValidFraction()));
        }
        
        // Hubness results
        HubnessMeasure.HubnessResult hubnessResult = report.getResult("Hubness", HubnessMeasure.HubnessResult.class);
        if (hubnessResult != null) {
            csv.append(String.format("Hubness_Skewness,%.4f\n", hubnessResult.skewness));
            csv.append(String.format("Hubness_HubCount,%d\n", hubnessResult.hubCount));
            csv.append(String.format("Hubness_AntiHubCount,%d\n", hubnessResult.antiHubCount));
            csv.append(String.format("Hubness_HubFraction,%.4f\n", hubnessResult.getHubFraction()));
            csv.append(String.format("Hubness_AntiHubFraction,%.4f\n", hubnessResult.getAntiHubFraction()));
            csv.append(String.format("Hubness_InDegreeMean,%.4f\n", hubnessResult.inDegreeStats.mean));
            csv.append(String.format("Hubness_InDegreeStdDev,%.4f\n", hubnessResult.inDegreeStats.stdDev));
        }
        
        return csv.toString();
    }

    /// Creates a JSON report of analysis results for programmatic processing.
    /// Produces well-formed JSON with nested structure for each measure.
    /// 
    /// @param report the analysis report
    /// @return JSON formatted string (pretty-printed)
    public static String toJsonReport(VectorSpaceAnalyzer.AnalysisReport report) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        
        // Basic vector space info
        json.append(String.format("  \"vectorSpaceId\": \"%s\",\n", report.vectorSpace.getId()));
        json.append(String.format("  \"vectorCount\": %d,\n", report.vectorSpace.getVectorCount()));
        json.append(String.format("  \"dimension\": %d,\n", report.vectorSpace.getDimension()));
        json.append(String.format("  \"hasClassLabels\": %s,\n", report.vectorSpace.hasClassLabels()));
        
        // Analysis results
        json.append("  \"results\": {\n");
        
        // LID results
        LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
        if (lidResult != null) {
            json.append("    \"LID\": {\n");
            json.append(String.format("      \"mean\": %.4f,\n", lidResult.statistics.mean));
            json.append(String.format("      \"stdDev\": %.4f,\n", lidResult.statistics.stdDev));
            json.append(String.format("      \"min\": %.4f,\n", lidResult.statistics.min));
            json.append(String.format("      \"max\": %.4f,\n", lidResult.statistics.max));
            json.append(String.format("      \"k\": %d\n", lidResult.k));
            json.append("    }");
        }
        
        // Margin results
        MarginMeasure.MarginResult marginResult = report.getResult("Margin", MarginMeasure.MarginResult.class);
        if (marginResult != null) {
            if (lidResult != null) json.append(",\n");
            json.append("    \"Margin\": {\n");
            json.append(String.format("      \"mean\": %.4f,\n", marginResult.statistics.mean));
            json.append(String.format("      \"stdDev\": %.4f,\n", marginResult.statistics.stdDev));
            json.append(String.format("      \"min\": %.4f,\n", marginResult.statistics.min));
            json.append(String.format("      \"max\": %.4f,\n", marginResult.statistics.max));
            json.append(String.format("      \"validCount\": %d,\n", marginResult.validCount));
            json.append(String.format("      \"validFraction\": %.4f\n", marginResult.getValidFraction()));
            json.append("    }");
        }
        
        // Hubness results
        HubnessMeasure.HubnessResult hubnessResult = report.getResult("Hubness", HubnessMeasure.HubnessResult.class);
        if (hubnessResult != null) {
            if (lidResult != null || marginResult != null) json.append(",\n");
            json.append("    \"Hubness\": {\n");
            json.append(String.format("      \"skewness\": %.4f,\n", hubnessResult.skewness));
            json.append(String.format("      \"hubCount\": %d,\n", hubnessResult.hubCount));
            json.append(String.format("      \"antiHubCount\": %d,\n", hubnessResult.antiHubCount));
            json.append(String.format("      \"hubFraction\": %.4f,\n", hubnessResult.getHubFraction()));
            json.append(String.format("      \"antiHubFraction\": %.4f,\n", hubnessResult.getAntiHubFraction()));
            json.append(String.format("      \"k\": %d,\n", hubnessResult.k));
            json.append("      \"inDegreeStats\": {\n");
            json.append(String.format("        \"mean\": %.4f,\n", hubnessResult.inDegreeStats.mean));
            json.append(String.format("        \"stdDev\": %.4f,\n", hubnessResult.inDegreeStats.stdDev));
            json.append(String.format("        \"min\": %.4f,\n", hubnessResult.inDegreeStats.min));
            json.append(String.format("        \"max\": %.4f\n", hubnessResult.inDegreeStats.max));
            json.append("      }\n");
            json.append("    }");
        }
        
        json.append("\n  }\n");
        json.append("}\n");
        
        return json.toString();
    }

    /// Provides human-readable interpretation of analysis results with insights.
    /// Translates numeric measures into actionable insights about data characteristics.
    /// 
    /// @param report the analysis report to interpret
    /// @return detailed multi-line interpretation with practical implications
    public static String interpretResults(VectorSpaceAnalyzer.AnalysisReport report) {
        StringBuilder interpretation = new StringBuilder();
        interpretation.append("Vector Space Analysis Interpretation\n");
        interpretation.append("===================================\n\n");
        
        // Basic characteristics
        interpretation.append(String.format("Dataset: %s (%d vectors, %d dimensions)\n\n",
                                           report.vectorSpace.getId(),
                                           report.vectorSpace.getVectorCount(),
                                           report.vectorSpace.getDimension()));
        
        // LID interpretation
        LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
        if (lidResult != null) {
            interpretation.append("Local Intrinsic Dimensionality (LID):\n");
            if (lidResult.statistics.mean < report.vectorSpace.getDimension() * 0.1) {
                interpretation.append("- Low intrinsic dimensionality suggests data lies on a low-dimensional manifold\n");
            } else if (lidResult.statistics.mean > report.vectorSpace.getDimension() * 0.8) {
                interpretation.append("- High intrinsic dimensionality indicates data fills the ambient space\n");
            } else {
                interpretation.append("- Moderate intrinsic dimensionality suggests structured but complex data\n");
            }
            
            if (lidResult.statistics.stdDev / lidResult.statistics.mean > 0.5) {
                interpretation.append("- High LID variance indicates non-uniform data density\n");
            }
            interpretation.append("\n");
        }
        
        // Margin interpretation
        MarginMeasure.MarginResult marginResult = report.getResult("Margin", MarginMeasure.MarginResult.class);
        if (marginResult != null && report.vectorSpace.hasClassLabels()) {
            interpretation.append("Class Separability (Margin):\n");
            if (marginResult.statistics.mean > 2.0) {
                interpretation.append("- High margin values indicate well-separated classes\n");
            } else if (marginResult.statistics.mean < 1.2) {
                interpretation.append("- Low margin values suggest overlapping or poorly separated classes\n");
            } else {
                interpretation.append("- Moderate margin values indicate reasonable class separation\n");
            }
            
            if (marginResult.getValidFraction() < 0.8) {
                interpretation.append("- Some vectors lack clear class separation\n");
            }
            interpretation.append("\n");
        }
        
        // Hubness interpretation
        HubnessMeasure.HubnessResult hubnessResult = report.getResult("Hubness", HubnessMeasure.HubnessResult.class);
        if (hubnessResult != null) {
            interpretation.append("Hubness Analysis:\n");
            if (hubnessResult.skewness > 1.0) {
                interpretation.append("- Positive skewness indicates presence of hub points\n");
                interpretation.append("- This may affect similarity-based algorithms negatively\n");
            } else if (hubnessResult.skewness < -1.0) {
                interpretation.append("- Negative skewness indicates many anti-hub points\n");
            } else {
                interpretation.append("- Low skewness suggests relatively uniform neighbor distribution\n");
            }
            
            if (hubnessResult.getHubFraction() > 0.1) {
                interpretation.append("- High proportion of hub points may cause bias in nearest neighbor methods\n");
            }
            
            if (hubnessResult.getAntiHubFraction() > 0.1) {
                interpretation.append("- High proportion of anti-hub points indicates isolated regions\n");
            }
        }
        
        return interpretation.toString();
    }
}