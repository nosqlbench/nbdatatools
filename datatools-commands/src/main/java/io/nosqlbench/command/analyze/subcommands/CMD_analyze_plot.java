package io.nosqlbench.command.analyze.subcommands;

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

import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vshapes.extract.BraillePlot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/// Plots vector data using Unicode braille characters for terminal visualization.
///
/// This command supports both histogram and scatter plot modes, with the ability
/// to overlay multiple files as separate series in different colors.
///
/// ## Usage Examples
///
/// ```bash
/// # Plot histogram of dimension 0
/// nbvectors analyze plot vectors.fvec -d 0
///
/// # Compare two files on dimension 3
/// nbvectors analyze plot file1.fvec file2.fvec -d 3
///
/// # Plot dimensions 0-4 from a file
/// nbvectors analyze plot vectors.fvec -d 0-4
///
/// # Scatter plot of dimension 0 vs 1
/// nbvectors analyze plot vectors.fvec --type scatter -d 0,1
///
/// # Custom size and bin count
/// nbvectors analyze plot vectors.fvec -d 0 --width 120 --height 30 --bins 100
/// ```
@CommandLine.Command(
    name = "plot",
    header = "Plot vector data using Unicode braille characters",
    description = "Visualizes dimension distributions as histograms or scatter plots with ANSI colors. " +
        "Multiple files can be overlaid as separate series for comparison.",
    exitCodeList = {"0: success", "1: error"}
)
public class CMD_analyze_plot implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_plot.class);

    /// Plot type enumeration.
    enum PlotType {
        histogram,  // Distribution histogram
        scatter     // Scatter plot (requires 2 dimensions)
    }

    // ============ Parameters and Options ============

    @CommandLine.Parameters(arity = "1..*", paramLabel = "FILE",
        description = "Vector file(s) to plot. Multiple files are shown as separate series with different colors.")
    private List<Path> files = new ArrayList<>();

    @CommandLine.Option(names = {"--type", "-t"},
        description = "Plot type: histogram (default) or scatter. " +
            "Scatter requires exactly 2 dimensions specified.")
    private PlotType plotType = PlotType.histogram;

    @CommandLine.Option(names = {"--dimensions", "-d"},
        description = "Dimensions to plot. Formats: single (0), range (0-5), or list (0,2,5). Default: 0")
    private String dimensions = "0";

    @CommandLine.Option(names = {"--width", "-w"},
        description = "Plot width in characters (default: 80)")
    private int width = 80;

    @CommandLine.Option(names = {"--height"},
        description = "Plot height in lines (default: 20)")
    private int height = 20;

    @CommandLine.Option(names = {"--bins", "-b"},
        description = "Number of histogram bins (default: auto based on width)")
    private Integer bins;

    @CommandLine.Option(names = {"--sample", "-s"},
        description = "Sample size per file. Use 0 for all data (default: 10000)")
    private int sampleSize = 10_000;

    @CommandLine.Option(names = {"--seed"},
        description = "Random seed for sampling (default: 42)")
    private long seed = 42;

    @CommandLine.Option(names = {"--no-legend"},
        description = "Hide the legend showing file names and statistics")
    private boolean noLegend = false;

    @CommandLine.Option(names = {"--no-stats"},
        description = "Hide statistics (min, max, mean, stddev) in legend")
    private boolean noStats = false;

    @Override
    public Integer call() {
        try {
            // Validate files exist
            for (Path file : files) {
                if (!Files.exists(file)) {
                    System.err.println("Error: File not found: " + file);
                    return 1;
                }
            }

            // Parse dimension specification
            List<Integer> dimList = parseDimensions(dimensions);
            if (dimList.isEmpty()) {
                System.err.println("Error: No valid dimensions specified");
                return 1;
            }

            // Scatter requires exactly 2 dimensions
            if (plotType == PlotType.scatter && dimList.size() != 2) {
                System.err.println("Error: Scatter plot requires exactly 2 dimensions (e.g., -d 0,1)");
                return 1;
            }

            // Load data from each file
            List<float[][]> allData = new ArrayList<>();  // [file][dim][values]
            List<String> labels = new ArrayList<>();

            for (Path file : files) {
                float[][] fileData = loadDimensionData(file, dimList);
                if (fileData == null) {
                    return 1;
                }
                allData.add(fileData);
                labels.add(file.getFileName().toString());
            }

            // Render plots
            if (plotType == PlotType.histogram) {
                renderHistograms(allData, labels, dimList);
            } else {
                renderScatterPlot(allData, labels, dimList);
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error plotting data", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Parses dimension specification into a list of dimension indices.
    /// Supports: single (0), range (0-5), or list (0,2,5).
    private List<Integer> parseDimensions(String spec) {
        List<Integer> result = new ArrayList<>();

        if (spec == null || spec.isEmpty()) {
            result.add(0);
            return result;
        }

        // Check for range format (0-5)
        if (spec.contains("-") && !spec.contains(",")) {
            String[] parts = spec.split("-");
            if (parts.length == 2) {
                try {
                    int start = Integer.parseInt(parts[0].trim());
                    int end = Integer.parseInt(parts[1].trim());
                    for (int i = start; i <= end; i++) {
                        result.add(i);
                    }
                    return result;
                } catch (NumberFormatException e) {
                    // Fall through to try other formats
                }
            }
        }

        // Check for list format (0,2,5)
        if (spec.contains(",")) {
            String[] parts = spec.split(",");
            for (String part : parts) {
                try {
                    result.add(Integer.parseInt(part.trim()));
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Invalid dimension: " + part);
                }
            }
            return result;
        }

        // Single dimension
        try {
            result.add(Integer.parseInt(spec.trim()));
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid dimension specification: " + spec);
        }

        return result;
    }

    /// Loads specified dimensions from a vector file.
    private float[][] loadDimensionData(Path file, List<Integer> dims) {
        try {
            // Determine file type from extension
            String fileName = file.getFileName().toString();
            String extension = fileName.substring(fileName.lastIndexOf('.') + 1);
            VectorFileExtension vfExt = VectorFileExtension.fromExtension(extension);
            if (vfExt == null) {
                System.err.println("Error: Unsupported file type: " + extension);
                return null;
            }
            FileType fileType = vfExt.getFileType();
            Class<?> dataType = vfExt.getDataType();

            VectorFileArray<?> vectorFile = VectorFileIO.randomAccess(fileType, dataType, file);
            int vectorCount = vectorFile.getSize();

            // Get dimensions from first vector
            int fileDims = 0;
            if (vectorCount > 0) {
                Object first = vectorFile.get(0);
                if (first instanceof float[] f) {
                    fileDims = f.length;
                } else if (first instanceof int[] i) {
                    fileDims = i.length;
                }
            }

            // Validate dimensions
            for (int d : dims) {
                if (d < 0 || d >= fileDims) {
                    System.err.println("Error: Dimension " + d + " out of range for file " +
                        file.getFileName() + " (has " + fileDims + " dimensions)");
                    return null;
                }
            }

            // Determine sample indices
            int[] indices;
            if (sampleSize <= 0 || sampleSize >= vectorCount) {
                indices = new int[vectorCount];
                for (int i = 0; i < vectorCount; i++) {
                    indices[i] = i;
                }
            } else {
                // Reservoir sampling
                indices = reservoirSample(vectorCount, sampleSize, seed);
            }

            // Load data for each dimension
            float[][] result = new float[dims.size()][indices.length];

            for (int i = 0; i < indices.length; i++) {
                Object vec = vectorFile.get(indices[i]);
                if (vec instanceof float[] fvec) {
                    for (int d = 0; d < dims.size(); d++) {
                        result[d][i] = fvec[dims.get(d)];
                    }
                }
            }

            System.out.printf("Loaded %,d vectors from %s (dims: %s)%n",
                indices.length, file.getFileName(), dims);

            return result;

        } catch (Exception e) {
            System.err.println("Error loading " + file + ": " + e.getMessage());
            return null;
        }
    }

    /// Performs reservoir sampling to select k random indices from n.
    private int[] reservoirSample(int n, int k, long seed) {
        int[] reservoir = new int[k];
        Random random = new Random(seed);

        // Fill reservoir with first k elements
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }

        // Replace elements with gradually decreasing probability
        for (int i = k; i < n; i++) {
            int j = random.nextInt(i + 1);
            if (j < k) {
                reservoir[j] = i;
            }
        }

        return reservoir;
    }

    /// Renders histogram plots for each dimension.
    private void renderHistograms(List<float[][]> allData, List<String> labels, List<Integer> dims) {
        int effectiveBins = (bins != null && bins > 0) ? bins : width * 2;

        for (int d = 0; d < dims.size(); d++) {
            int dimIndex = dims.get(d);

            System.out.println();
            System.out.println("Histogram: dimension " + dimIndex);
            System.out.println("─".repeat(width + 10));

            if (allData.size() == 1) {
                // Single series - no colors needed
                float[] data = allData.get(0)[d];
                System.out.print(BraillePlot.histogram(data, width, height, effectiveBins));

                if (!noLegend) {
                    printLegend(List.of(data), labels);
                }
            } else {
                // Multiple series - use colors
                List<float[]> seriesData = new ArrayList<>();
                for (float[][] fileData : allData) {
                    seriesData.add(fileData[d]);
                }

                System.out.print(BraillePlot.multiHistogram(seriesData,
                    noLegend ? null : labels, width, height, effectiveBins));

                if (!noLegend) {
                    printLegend(seriesData, labels);
                }
            }
        }
    }

    /// Renders scatter plot (requires exactly 2 dimensions).
    private void renderScatterPlot(List<float[][]> allData, List<String> labels, List<Integer> dims) {
        int xDim = dims.get(0);
        int yDim = dims.get(1);

        System.out.println();
        System.out.println("Scatter: dimension " + xDim + " vs " + yDim);
        System.out.println("─".repeat(width + 10));

        if (allData.size() == 1) {
            // Single series
            float[] x = allData.get(0)[0];
            float[] y = allData.get(0)[1];
            System.out.print(BraillePlot.scatter(x, y, width, height));

            if (!noLegend) {
                System.out.println();
                System.out.printf("   %s (n=%,d)%n", labels.get(0), x.length);
            }
        } else {
            // Multiple series
            List<float[]> xSeries = new ArrayList<>();
            List<float[]> ySeries = new ArrayList<>();

            for (float[][] fileData : allData) {
                xSeries.add(fileData[0]);
                ySeries.add(fileData[1]);
            }

            System.out.print(BraillePlot.multiScatter(xSeries, ySeries,
                noLegend ? null : labels, width, height));
        }
    }

    /// Prints legend with statistics for each series.
    private void printLegend(List<float[]> seriesData, List<String> labels) {
        if (noStats) return;

        System.out.println();
        for (int i = 0; i < seriesData.size(); i++) {
            float[] data = seriesData.get(i);
            double[] stats = BraillePlot.computeStats(data);

            String label = i < labels.size() ? labels.get(i) : "Series " + (i + 1);
            String color = BraillePlot.SERIES_COLORS[i % BraillePlot.SERIES_COLORS.length];

            System.out.printf("   %s●%s %s (n=%,d, μ=%.3f, σ=%.3f, [%.3f, %.3f])%n",
                color, BraillePlot.RESET,
                label, data.length,
                stats[2], stats[3],  // mean, stddev
                stats[0], stats[1]); // min, max
        }
    }
}
