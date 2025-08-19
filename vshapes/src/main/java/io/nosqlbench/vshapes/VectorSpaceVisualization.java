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

import java.awt.Color;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Visualization utilities for vector space analysis using Jzy3D.
 * 
 * Note: This class requires Jzy3D dependencies to be available on the classpath.
 * The dependencies are marked as optional in the POM to avoid forcing users
 * to include visualization libraries if not needed.
 */
public final class VectorSpaceVisualization {

    private VectorSpaceVisualization() {} // Utility class

    /**
     * Checks if visualization dependencies are available.
     * @return true if Jzy3D is available on the classpath
     */
    public static boolean isVisualizationAvailable() {
        try {
            Class.forName("org.jzy3d.chart.Chart");
            Class.forName("org.jzy3d.chart.factories.AWTChartFactory");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Creates a 3D scatter plot of vectors colored by LID values.
     * Requires vectors to be 3D or uses first 3 dimensions of higher-dimensional vectors.
     * 
     * @param vectorSpace the vector space to visualize
     * @param lidResult the LID analysis results
     * @param title title for the chart
     * @return the Jzy3D chart object, or null if visualization is not available
     */
    public static Object createLIDScatterPlot(VectorSpace vectorSpace, LIDMeasure.LIDResult lidResult, String title) {
        if (!isVisualizationAvailable()) {
            System.err.println("Warning: Jzy3D visualization dependencies not available");
            return null;
        }

        try {
            return createLIDScatterPlotInternal(vectorSpace, lidResult, title);
        } catch (Exception e) {
            System.err.println("Error creating LID scatter plot: " + e.getMessage());
            return null;
        }
    }

    /**
     * Creates a 3D scatter plot of vectors colored by class labels.
     * Requires vectors to be 3D or uses first 3 dimensions of higher-dimensional vectors.
     * 
     * @param vectorSpace the vector space to visualize (must have class labels)
     * @param title title for the chart
     * @return the Jzy3D chart object, or null if visualization is not available
     */
    public static Object createClassScatterPlot(VectorSpace vectorSpace, String title) {
        if (!isVisualizationAvailable()) {
            System.err.println("Warning: Jzy3D visualization dependencies not available");
            return null;
        }

        if (!vectorSpace.hasClassLabels()) {
            System.err.println("Error: Vector space must have class labels for class visualization");
            return null;
        }

        try {
            return createClassScatterPlotInternal(vectorSpace, title);
        } catch (Exception e) {
            System.err.println("Error creating class scatter plot: " + e.getMessage());
            return null;
        }
    }

    /**
     * Creates a 3D scatter plot of vectors sized by hubness scores.
     * Hub points appear larger, anti-hub points appear smaller.
     * 
     * @param vectorSpace the vector space to visualize
     * @param hubnessResult the hubness analysis results
     * @param title title for the chart
     * @return the Jzy3D chart object, or null if visualization is not available
     */
    public static Object createHubnessScatterPlot(VectorSpace vectorSpace, HubnessMeasure.HubnessResult hubnessResult, String title) {
        if (!isVisualizationAvailable()) {
            System.err.println("Warning: Jzy3D visualization dependencies not available");
            return null;
        }

        try {
            return createHubnessScatterPlotInternal(vectorSpace, hubnessResult, title);
        } catch (Exception e) {
            System.err.println("Error creating hubness scatter plot: " + e.getMessage());
            return null;
        }
    }

    /**
     * Saves a chart as an image file.
     * 
     * @param chart the chart object (should be a Jzy3D Chart)
     * @param outputPath path to save the image
     * @param width image width in pixels
     * @param height image height in pixels
     * @return true if successful, false otherwise
     */
    public static boolean saveChartAsImage(Object chart, Path outputPath, int width, int height) {
        if (!isVisualizationAvailable() || chart == null) {
            return false;
        }

        try {
            return saveChartAsImageInternal(chart, outputPath, width, height);
        } catch (Exception e) {
            System.err.println("Error saving chart: " + e.getMessage());
            return false;
        }
    }

    /**
     * Creates a comprehensive visualization dashboard for a vector space analysis.
     * Creates multiple plots showing different aspects of the analysis.
     * 
     * @param vectorSpace the vector space to visualize
     * @param report the analysis report
     * @param outputDir directory to save visualization files
     * @return true if successful, false otherwise
     */
    public static boolean createAnalysisDashboard(VectorSpace vectorSpace, VectorSpaceAnalyzer.AnalysisReport report, Path outputDir) {
        if (!isVisualizationAvailable()) {
            System.err.println("Warning: Jzy3D visualization dependencies not available");
            return false;
        }

        try {
            return createAnalysisDashboardInternal(vectorSpace, report, outputDir);
        } catch (Exception e) {
            System.err.println("Error creating analysis dashboard: " + e.getMessage());
            return false;
        }
    }

    // Internal implementation methods using reflection to avoid compile-time dependencies

    private static Object createLIDScatterPlotInternal(VectorSpace vectorSpace, LIDMeasure.LIDResult lidResult, String title) throws Exception {
        // Use reflection to create Jzy3D chart
        Class<?> chartFactoryClass = Class.forName("org.jzy3d.chart.factories.AWTChartFactory");
        Class<?> chartClass = Class.forName("org.jzy3d.chart.Chart");
        Class<?> scatterClass = Class.forName("org.jzy3d.plot3d.primitives.Scatter");
        Class<?> coord3dClass = Class.forName("org.jzy3d.maths.Coord3d");
        Class<?> colorClass = Class.forName("org.jzy3d.colors.Color");
        
        Object factory = chartFactoryClass.getMethod("newInstance").invoke(null);
        Object chart = chartFactoryClass.getMethod("newChart").invoke(factory);
        
        // Prepare data points
        int n = vectorSpace.getVectorCount();
        int dims = Math.min(3, vectorSpace.getDimension());
        
        Object[] points = new Object[n];
        Object[] colors = new Object[n];
        
        // Get LID statistics for color mapping
        double minLID = lidResult.statistics.min;
        double maxLID = lidResult.statistics.max;
        double lidRange = maxLID - minLID;
        
        for (int i = 0; i < n; i++) {
            float[] vector = vectorSpace.getVector(i);
            
            // Create 3D coordinate (pad with zeros if needed)
            float x = dims > 0 ? vector[0] : 0;
            float y = dims > 1 ? vector[1] : 0;
            float z = dims > 2 ? vector[2] : 0;
            
            points[i] = coord3dClass.getConstructor(float.class, float.class, float.class)
                                  .newInstance(x, y, z);
            
            // Map LID to color (blue = low LID, red = high LID)
            double lid = lidResult.getLID(i);
            float colorRatio = lidRange > 0 ? (float) ((lid - minLID) / lidRange) : 0;
            colors[i] = colorClass.getConstructor(float.class, float.class, float.class, float.class)
                                 .newInstance(colorRatio, 0.0f, 1.0f - colorRatio, 0.8f);
        }
        
        // Create scatter plot
        Object scatter = scatterClass.getConstructor(points.getClass(), colors.getClass())
                                   .newInstance(points, colors);
        
        // Add to chart
        chartClass.getMethod("add", Class.forName("org.jzy3d.plot3d.primitives.Drawable"))
                  .invoke(chart, scatter);
        
        // Set title
        chartClass.getMethod("setTitle", String.class).invoke(chart, title);
        
        return chart;
    }

    private static Object createClassScatterPlotInternal(VectorSpace vectorSpace, String title) throws Exception {
        Class<?> chartFactoryClass = Class.forName("org.jzy3d.chart.factories.AWTChartFactory");
        Class<?> chartClass = Class.forName("org.jzy3d.chart.Chart");
        Class<?> scatterClass = Class.forName("org.jzy3d.plot3d.primitives.Scatter");
        Class<?> coord3dClass = Class.forName("org.jzy3d.maths.Coord3d");
        Class<?> colorClass = Class.forName("org.jzy3d.colors.Color");
        
        Object factory = chartFactoryClass.getMethod("newInstance").invoke(null);
        Object chart = chartFactoryClass.getMethod("newChart").invoke(factory);
        
        int n = vectorSpace.getVectorCount();
        int dims = Math.min(3, vectorSpace.getDimension());
        
        Object[] points = new Object[n];
        Object[] colors = new Object[n];
        
        // Predefined colors for different classes
        Color[] classColors = {
            Color.RED, Color.BLUE, Color.GREEN, Color.YELLOW, Color.MAGENTA,
            Color.CYAN, Color.ORANGE, Color.PINK, Color.GRAY, Color.BLACK
        };
        
        for (int i = 0; i < n; i++) {
            float[] vector = vectorSpace.getVector(i);
            
            float x = dims > 0 ? vector[0] : 0;
            float y = dims > 1 ? vector[1] : 0;
            float z = dims > 2 ? vector[2] : 0;
            
            points[i] = coord3dClass.getConstructor(float.class, float.class, float.class)
                                  .newInstance(x, y, z);
            
            // Get class label and map to color
            Optional<Integer> classLabel = vectorSpace.getClassLabel(i);
            Color javaColor = classLabel.isPresent() ? 
                            classColors[classLabel.get() % classColors.length] : 
                            Color.LIGHT_GRAY;
            
            colors[i] = colorClass.getConstructor(float.class, float.class, float.class, float.class)
                                 .newInstance(javaColor.getRed() / 255.0f,
                                            javaColor.getGreen() / 255.0f,
                                            javaColor.getBlue() / 255.0f, 0.8f);
        }
        
        Object scatter = scatterClass.getConstructor(points.getClass(), colors.getClass())
                                   .newInstance(points, colors);
        
        chartClass.getMethod("add", Class.forName("org.jzy3d.plot3d.primitives.Drawable"))
                  .invoke(chart, scatter);
        
        chartClass.getMethod("setTitle", String.class).invoke(chart, title);
        
        return chart;
    }

    private static Object createHubnessScatterPlotInternal(VectorSpace vectorSpace, HubnessMeasure.HubnessResult hubnessResult, String title) throws Exception {
        Class<?> chartFactoryClass = Class.forName("org.jzy3d.chart.factories.AWTChartFactory");
        Class<?> chartClass = Class.forName("org.jzy3d.chart.Chart");
        Class<?> scatterClass = Class.forName("org.jzy3d.plot3d.primitives.Scatter");
        Class<?> coord3dClass = Class.forName("org.jzy3d.maths.Coord3d");
        Class<?> colorClass = Class.forName("org.jzy3d.colors.Color");
        
        Object factory = chartFactoryClass.getMethod("newInstance").invoke(null);
        Object chart = chartFactoryClass.getMethod("newChart").invoke(factory);
        
        int n = vectorSpace.getVectorCount();
        int dims = Math.min(3, vectorSpace.getDimension());
        
        Object[] points = new Object[n];
        Object[] colors = new Object[n];
        
        for (int i = 0; i < n; i++) {
            float[] vector = vectorSpace.getVector(i);
            
            float x = dims > 0 ? vector[0] : 0;
            float y = dims > 1 ? vector[1] : 0;
            float z = dims > 2 ? vector[2] : 0;
            
            points[i] = coord3dClass.getConstructor(float.class, float.class, float.class)
                                  .newInstance(x, y, z);
            
            // Color by hubness: red=hub, blue=anti-hub, gray=normal
            double hubnessScore = hubnessResult.getHubnessScore(i);
            float red, green, blue;
            
            if (hubnessResult.isHub(i)) {
                red = 1.0f; green = 0.0f; blue = 0.0f; // Red for hubs
            } else if (hubnessResult.isAntiHub(i)) {
                red = 0.0f; green = 0.0f; blue = 1.0f; // Blue for anti-hubs
            } else {
                red = 0.5f; green = 0.5f; blue = 0.5f; // Gray for normal points
            }
            
            colors[i] = colorClass.getConstructor(float.class, float.class, float.class, float.class)
                                 .newInstance(red, green, blue, 0.8f);
        }
        
        Object scatter = scatterClass.getConstructor(points.getClass(), colors.getClass())
                                   .newInstance(points, colors);
        
        chartClass.getMethod("add", Class.forName("org.jzy3d.plot3d.primitives.Drawable"))
                  .invoke(chart, scatter);
        
        chartClass.getMethod("setTitle", String.class).invoke(chart, title);
        
        return chart;
    }

    private static boolean saveChartAsImageInternal(Object chart, Path outputPath, int width, int height) throws Exception {
        Class<?> chartClass = Class.forName("org.jzy3d.chart.Chart");
        
        // Take screenshot
        chartClass.getMethod("screenshot", String.class)
                  .invoke(chart, outputPath.toString());
        
        return true;
    }

    private static boolean createAnalysisDashboardInternal(VectorSpace vectorSpace, VectorSpaceAnalyzer.AnalysisReport report, Path outputDir) throws Exception {
        try {
            java.nio.file.Files.createDirectories(outputDir);
            
            boolean success = true;
            
            // Create LID visualization if available
            LIDMeasure.LIDResult lidResult = report.getResult("LID", LIDMeasure.LIDResult.class);
            if (lidResult != null) {
                Object lidChart = createLIDScatterPlotInternal(vectorSpace, lidResult, 
                    "Local Intrinsic Dimensionality - " + vectorSpace.getId());
                if (lidChart != null) {
                    success &= saveChartAsImageInternal(lidChart, outputDir.resolve("lid_visualization.png"), 800, 600);
                }
            }
            
            // Create class visualization if available
            if (vectorSpace.hasClassLabels()) {
                Object classChart = createClassScatterPlotInternal(vectorSpace, 
                    "Class Distribution - " + vectorSpace.getId());
                if (classChart != null) {
                    success &= saveChartAsImageInternal(classChart, outputDir.resolve("class_visualization.png"), 800, 600);
                }
            }
            
            // Create hubness visualization if available
            HubnessMeasure.HubnessResult hubnessResult = report.getResult("Hubness", HubnessMeasure.HubnessResult.class);
            if (hubnessResult != null) {
                Object hubnessChart = createHubnessScatterPlotInternal(vectorSpace, hubnessResult, 
                    "Hubness Analysis - " + vectorSpace.getId());
                if (hubnessChart != null) {
                    success &= saveChartAsImageInternal(hubnessChart, outputDir.resolve("hubness_visualization.png"), 800, 600);
                }
            }
            
            return success;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directory", e);
        }
    }

    /**
     * Display a chart in a window (requires GUI environment).
     * 
     * @param chart the chart to display
     * @return true if successful, false otherwise
     */
    public static boolean displayChart(Object chart) {
        if (!isVisualizationAvailable() || chart == null) {
            return false;
        }

        try {
            Class<?> chartClass = Class.forName("org.jzy3d.chart.Chart");
            chartClass.getMethod("open", int.class, int.class).invoke(chart, 800, 600);
            return true;
        } catch (Exception e) {
            System.err.println("Error displaying chart: " + e.getMessage());
            return false;
        }
    }
}