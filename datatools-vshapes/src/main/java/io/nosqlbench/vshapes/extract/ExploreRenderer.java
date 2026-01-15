package io.nosqlbench.vshapes.extract;

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

import java.util.*;

/// Renders the interactive explore command display using braille characters.
///
/// This class is separated from the command to enable headless testing
/// with TerminalTestRig.
///
/// ## Layout Structure
///
/// ```
/// ════════════════════════════════════════════════════════════════════════════════
///   VECTOR EXPLORER: filename.fvec | 10,000 vectors × 128 dimensions
///   Current: dim 0 | Viewing: dim 0
/// ════════════════════════════════════════════════════════════════════════════════
///   1.0 ⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
///       ⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
///       ...
///   0.0 ⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀⣀
///       └────────────────────────────────────────────────────────────────────────
///       -1.000                                                            1.000
///
///   ● Dimension 0 ◄
///
///   ←/→: prev/next dim | Space: toggle | r: reset | q: quit
/// ```
public final class ExploreRenderer {

    // ANSI escape codes
    public static final String RESET = "\u001B[0m";
    public static final String CLEAR_SCREEN = "\u001B[2J";
    public static final String CURSOR_HOME = "\u001B[H";
    public static final String CURSOR_HIDE = "\u001B[?25l";
    public static final String CURSOR_SHOW = "\u001B[?25h";
    public static final String CLEAR_LINE = "\u001B[K";

    // Line ending for terminal output (CR+LF for proper VT100/ANSI behavior)
    private static final String NL = "\r\n";

    // Braille constants
    private static final int BRAILLE_BASE = 0x2800;
    private static final int[] LEFT_COLUMN_BITS = {0x01, 0x02, 0x04, 0x40};
    private static final int[] RIGHT_COLUMN_BITS = {0x08, 0x10, 0x20, 0x80};

    private final int width;
    private final int height;
    private final int bins;

    /// Creates a renderer with specified dimensions.
    ///
    /// @param width plot width in characters
    /// @param height plot height in lines
    public ExploreRenderer(int width, int height) {
        this.width = width;
        this.height = height;
        this.bins = width * 2;
    }

    /// Renders the full explore display frame.
    ///
    /// @param state the current explore state
    /// @return ANSI-encoded frame string
    public String renderFrame(ExploreState state) {
        StringBuilder sb = new StringBuilder();

        // Position cursor at home
        sb.append(CURSOR_HOME);

        // Header
        sb.append("═".repeat(width + 10)).append(CLEAR_LINE).append(NL);
        sb.append(String.format("  VECTOR EXPLORER: %s | %,d vectors × %d dimensions",
            state.fileName(), state.vectorCount(), state.fileDimensions()));
        sb.append(CLEAR_LINE).append(NL);
        sb.append(String.format("  Current: dim %d | Viewing: %s",
            state.currentDimension(), formatEnabledDimensions(state.enabledDimensions())));
        sb.append(CLEAR_LINE).append(NL);
        sb.append("═".repeat(width + 10)).append(CLEAR_LINE).append(NL);

        // Plot area
        renderHistogram(sb, state);

        // X-axis
        sb.append("      └").append("─".repeat(width)).append(CLEAR_LINE).append(NL);
        sb.append(String.format("      %-10.3f", state.globalMin()));
        sb.append(" ".repeat(Math.max(0, width - 20)));
        sb.append(String.format("%10.3f", state.globalMax()));
        sb.append(CLEAR_LINE).append(NL);

        // Legend
        sb.append(CLEAR_LINE).append(NL);
        for (int dim : state.enabledDimensions()) {
            sb.append("  ").append(get24BitColor(dim)).append("●").append(RESET);
            sb.append(String.format(" Dimension %d", dim));
            if (dim == state.currentDimension()) sb.append(" ◄");
            sb.append(CLEAR_LINE).append(NL);
        }

        // Loading status
        sb.append(CLEAR_LINE).append(NL);
        sb.append("  ").append(formatLoadingStatus(state.samplesLoaded(), state.targetSamples()));
        sb.append(CLEAR_LINE).append(NL);

        // Model string (if available)
        if (state.modelString() != null && !state.modelString().isEmpty()) {
            sb.append(CLEAR_LINE).append(NL);
            sb.append("  ").append(state.modelString());
            sb.append(CLEAR_LINE).append(NL);
        }

        // Controls
        sb.append(CLEAR_LINE).append(NL);
        sb.append("  ←/→: dim | +/-: samples | Space: toggle | m: model | g: grid | r: reset | q: quit");
        sb.append(CLEAR_LINE).append(NL);

        return sb.toString();
    }

    /// Formats the loading status line with progress bar.
    private String formatLoadingStatus(int loaded, int target) {
        if (target <= 0) {
            return "Samples: 0";
        }

        int percent = (int) ((loaded * 100L) / target);
        String status;
        if (loaded >= target) {
            status = String.format("Samples: %,d/%,d (100%%)", loaded, target);
        } else {
            status = String.format("Samples: %,d/%,d (%d%%) loading...", loaded, target, percent);
        }

        // Progress bar (10 chars wide)
        int barWidth = 10;
        int filled = (int) ((loaded * (long) barWidth) / target);
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < barWidth; i++) {
            bar.append(i < filled ? "█" : "░");
        }
        bar.append("]");

        return status + " " + bar;
    }

    /// Renders the histogram portion of the display.
    private void renderHistogram(StringBuilder sb, ExploreState state) {
        int gridHeight = height * 4;

        // Find max count across all enabled dimensions
        int maxCount = 0;
        for (int dim : state.enabledDimensions()) {
            int[] counts = state.histogramCounts().get(dim);
            if (counts != null) {
                for (int c : counts) {
                    if (c > maxCount) maxCount = c;
                }
            }
        }

        // Render each row
        for (int row = 0; row < height; row++) {
            // Y-axis label
            if (row == 0) {
                sb.append(String.format("%5.1f ", 1.0));
            } else if (row == height - 1) {
                sb.append(String.format("%5.1f ", 0.0));
            } else {
                sb.append("      ");
            }

            // Render braille characters
            for (int col = 0; col < width; col++) {
                int combinedBits = 0;
                int dominantDim = -1;
                int maxBitCount = 0;

                for (int dim : state.enabledDimensions()) {
                    int[] counts = state.histogramCounts().get(dim);
                    if (counts == null) continue;

                    int dimBits = 0;
                    for (int dx = 0; dx < 2; dx++) {
                        int binIdx = col * 2 + dx;
                        if (binIdx >= bins) continue;

                        double barHeight = maxCount > 0 ? (double) counts[binIdx] / maxCount : 0;
                        int filledRows = (int) (barHeight * gridHeight);

                        for (int dy = 0; dy < 4; dy++) {
                            int gridRow = row * 4 + dy;
                            int rowFromBottom = gridHeight - 1 - gridRow;

                            if (rowFromBottom < filledRows) {
                                dimBits |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                            }
                        }
                    }

                    combinedBits |= dimBits;
                    int bitCount = Integer.bitCount(dimBits);
                    if (bitCount > maxBitCount) {
                        maxBitCount = bitCount;
                        dominantDim = dim;
                    }
                }

                if (dominantDim >= 0 && combinedBits > 0) {
                    sb.append(get24BitColor(dominantDim));
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                    sb.append(RESET);
                } else {
                    sb.append((char) (BRAILLE_BASE + combinedBits));
                }
            }
            sb.append(CLEAR_LINE).append(NL);
        }
    }

    /// Generates a 24-bit ANSI color code for a dimension.
    ///
    /// Uses HSL color space with golden angle distribution for
    /// visually distinct colors.
    ///
    /// @param dim dimension index
    /// @return ANSI escape sequence for foreground color
    public static String get24BitColor(int dim) {
        float hue = (dim * 137.5f) % 360;
        float sat = 0.8f;
        float lit = 0.6f;

        float c = (1 - Math.abs(2 * lit - 1)) * sat;
        float x = c * (1 - Math.abs((hue / 60) % 2 - 1));
        float m = lit - c / 2;

        float r, g, b;
        if (hue < 60) { r = c; g = x; b = 0; }
        else if (hue < 120) { r = x; g = c; b = 0; }
        else if (hue < 180) { r = 0; g = c; b = x; }
        else if (hue < 240) { r = 0; g = x; b = c; }
        else if (hue < 300) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }

        int ri = (int) ((r + m) * 255);
        int gi = (int) ((g + m) * 255);
        int bi = (int) ((b + m) * 255);

        return String.format("\u001B[38;2;%d;%d;%dm", ri, gi, bi);
    }

    private String formatEnabledDimensions(Set<Integer> enabledDimensions) {
        if (enabledDimensions.size() == 1) {
            return "dim " + enabledDimensions.iterator().next();
        }
        return enabledDimensions.size() + " dims: " +
            enabledDimensions.stream()
                .map(String::valueOf)
                .reduce((a, b) -> a + "," + b)
                .orElse("");
    }

    /// Returns the number of histogram bins.
    public int getBins() {
        return bins;
    }

    /// Truncates a filename to fit within maxLen characters.
    ///
    /// @param fileName the filename to truncate
    /// @param maxLen maximum length
    /// @return truncated filename with "..." if too long
    private static String truncateFileName(String fileName, int maxLen) {
        if (fileName == null) return "unknown";
        if (fileName.length() <= maxLen) return fileName;
        return fileName.substring(0, maxLen - 3) + "...";
    }

    /// Renders a grid of histograms for multiple dimensions.
    ///
    /// @param state the grid state containing layout and data
    /// @return ANSI-encoded frame string
    public String renderGridFrame(GridState state) {
        StringBuilder sb = new StringBuilder();

        // Position cursor at home
        sb.append(CURSOR_HOME);

        // Calculate available width for the grid (6 chars y-axis + plotWidth, plus 1 separator between cols)
        int gridWidth = state.gridColumns() * (state.plotWidth() + 6) + (state.gridColumns() - 1);

        // Compact header that fits within the grid width
        String header = String.format("GRID: %s | %,dv×%dd | %,d/%,d | dims %d-%d",
            truncateFileName(state.fileName(), 15),
            state.vectorCount(), state.fileDimensions(),
            state.samplesLoaded(), state.targetSamples(),
            state.startDimension(), state.startDimension() + (state.gridRows() * state.gridColumns()) - 1);
        // Truncate header to fit in grid width minus 2 for margins
        if (header.length() > gridWidth - 2) {
            header = header.substring(0, gridWidth - 5) + "...";
        }
        sb.append("  ").append(header);
        sb.append(CLEAR_LINE).append(NL);
        sb.append("═".repeat(gridWidth)).append(CLEAR_LINE).append(NL);

        // Render grid rows
        for (int gridRow = 0; gridRow < state.gridRows(); gridRow++) {
            renderGridRow(sb, state, gridRow);
        }

        // Controls (no trailing NL to avoid overflow)
        sb.append(CLEAR_LINE).append(NL);
        sb.append("  ←/→: scroll dims | g: single view | +/-: samples | q: quit");
        sb.append(CLEAR_LINE);

        return sb.toString();
    }

    /// Renders a single row of the grid (multiple histograms side by side).
    private void renderGridRow(StringBuilder sb, GridState state, int gridRow) {
        int plotHeight = state.plotHeight();
        int plotWidth = state.plotWidth();
        int gridHeight = plotHeight * 4;
        int gridBins = plotWidth * 2;

        // Dimension title row
        for (int gridCol = 0; gridCol < state.gridColumns(); gridCol++) {
            int dim = state.startDimension() + gridRow * state.gridColumns() + gridCol;
            if (dim < state.fileDimensions()) {
                String title = String.format(" dim %d", dim);
                sb.append(get24BitColor(dim));
                sb.append(String.format("%-" + (plotWidth + 6) + "s", title));
                sb.append(RESET);
            } else {
                sb.append(" ".repeat(plotWidth + 6));
            }
            if (gridCol < state.gridColumns() - 1) sb.append(" ");
        }
        sb.append(CLEAR_LINE).append(NL);

        // Render histogram rows
        for (int row = 0; row < plotHeight; row++) {
            for (int gridCol = 0; gridCol < state.gridColumns(); gridCol++) {
                int dim = state.startDimension() + gridRow * state.gridColumns() + gridCol;

                // Y-axis label
                if (row == 0) {
                    sb.append(String.format("%5.1f ", 1.0));
                } else if (row == plotHeight - 1) {
                    sb.append(String.format("%5.1f ", 0.0));
                } else {
                    sb.append("      ");
                }

                if (dim < state.fileDimensions()) {
                    int[] counts = state.histogramCounts().get(dim);
                    int maxCount = 0;
                    if (counts != null) {
                        for (int c : counts) {
                            if (c > maxCount) maxCount = c;
                        }
                    }

                    // Render braille characters for this row
                    for (int col = 0; col < plotWidth; col++) {
                        int bits = 0;
                        if (counts != null && maxCount > 0) {
                            for (int dx = 0; dx < 2; dx++) {
                                int binIdx = col * 2 + dx;
                                if (binIdx >= gridBins) continue;

                                double barHeight = (double) counts[binIdx] / maxCount;
                                int filledRows = (int) (barHeight * gridHeight);

                                for (int dy = 0; dy < 4; dy++) {
                                    int gridRowIdx = row * 4 + dy;
                                    int rowFromBottom = gridHeight - 1 - gridRowIdx;

                                    if (rowFromBottom < filledRows) {
                                        bits |= (dx == 0 ? LEFT_COLUMN_BITS[dy] : RIGHT_COLUMN_BITS[dy]);
                                    }
                                }
                            }
                        }
                        if (bits > 0) {
                            sb.append(get24BitColor(dim));
                            sb.append((char) (BRAILLE_BASE + bits));
                            sb.append(RESET);
                        } else {
                            sb.append((char) BRAILLE_BASE);
                        }
                    }
                } else {
                    sb.append(" ".repeat(plotWidth));
                }

                if (gridCol < state.gridColumns() - 1) sb.append(" ");
            }
            sb.append(CLEAR_LINE).append(NL);
        }

        // X-axis row (6 chars label + 1 corner + (plotWidth-1) dashes = 6 + plotWidth total)
        for (int gridCol = 0; gridCol < state.gridColumns(); gridCol++) {
            int dim = state.startDimension() + gridRow * state.gridColumns() + gridCol;
            if (dim < state.fileDimensions()) {
                sb.append("      └").append("─".repeat(plotWidth - 1));
            } else {
                sb.append(" ".repeat(plotWidth + 6));
            }
            if (gridCol < state.gridColumns() - 1) sb.append(" ");
        }
        sb.append(CLEAR_LINE).append(NL);

        // X-axis labels row
        for (int gridCol = 0; gridCol < state.gridColumns(); gridCol++) {
            int dim = state.startDimension() + gridRow * state.gridColumns() + gridCol;
            if (dim < state.fileDimensions()) {
                sb.append(String.format("      %-" + (plotWidth / 2) + ".2f", state.globalMin()));
                sb.append(String.format("%" + (plotWidth - plotWidth / 2) + ".2f", state.globalMax()));
            } else {
                sb.append(" ".repeat(plotWidth + 6));
            }
            if (gridCol < state.gridColumns() - 1) sb.append(" ");
        }
        sb.append(CLEAR_LINE).append(NL);
    }

    /// Returns histogram bin count for a specific plot width.
    public static int getBinsForWidth(int plotWidth) {
        return plotWidth * 2;
    }

    /// Immutable state record for the explore display.
    ///
    /// @param fileName source file name
    /// @param vectorCount total number of vectors in the file
    /// @param fileDimensions number of dimensions per vector
    /// @param currentDimension currently selected dimension
    /// @param enabledDimensions set of dimensions being displayed
    /// @param histogramCounts histogram bin counts per dimension
    /// @param globalMin minimum value across all displayed dimensions
    /// @param globalMax maximum value across all displayed dimensions
    /// @param samplesLoaded number of samples currently loaded
    /// @param targetSamples target number of samples to load
    /// @param modelString extracted model string (null if not extracted)
    public record ExploreState(
        String fileName,
        int vectorCount,
        int fileDimensions,
        int currentDimension,
        Set<Integer> enabledDimensions,
        Map<Integer, int[]> histogramCounts,
        float globalMin,
        float globalMax,
        int samplesLoaded,
        int targetSamples,
        String modelString
    ) {
        /// Creates a builder for ExploreState.
        public static Builder builder() {
            return new Builder();
        }

        /// Builder for ExploreState.
        public static class Builder {
            private String fileName = "unknown";
            private int vectorCount = 0;
            private int fileDimensions = 0;
            private int currentDimension = 0;
            private Set<Integer> enabledDimensions = new LinkedHashSet<>();
            private Map<Integer, int[]> histogramCounts = new HashMap<>();
            private float globalMin = -1.0f;
            private float globalMax = 1.0f;
            private int samplesLoaded = 0;
            private int targetSamples = 0;
            private String modelString = null;

            public Builder fileName(String fileName) { this.fileName = fileName; return this; }
            public Builder vectorCount(int vectorCount) { this.vectorCount = vectorCount; return this; }
            public Builder fileDimensions(int fileDimensions) { this.fileDimensions = fileDimensions; return this; }
            public Builder currentDimension(int currentDimension) { this.currentDimension = currentDimension; return this; }
            public Builder enabledDimensions(Set<Integer> enabledDimensions) { this.enabledDimensions = enabledDimensions; return this; }
            public Builder histogramCounts(Map<Integer, int[]> histogramCounts) { this.histogramCounts = histogramCounts; return this; }
            public Builder globalMin(float globalMin) { this.globalMin = globalMin; return this; }
            public Builder globalMax(float globalMax) { this.globalMax = globalMax; return this; }
            public Builder samplesLoaded(int samplesLoaded) { this.samplesLoaded = samplesLoaded; return this; }
            public Builder targetSamples(int targetSamples) { this.targetSamples = targetSamples; return this; }
            public Builder modelString(String modelString) { this.modelString = modelString; return this; }

            public ExploreState build() {
                return new ExploreState(fileName, vectorCount, fileDimensions, currentDimension,
                    enabledDimensions, histogramCounts, globalMin, globalMax, samplesLoaded, targetSamples,
                    modelString);
            }
        }
    }

    /// Immutable state record for grid mode display.
    ///
    /// @param fileName source file name
    /// @param vectorCount total number of vectors in the file
    /// @param fileDimensions number of dimensions per vector
    /// @param startDimension first dimension shown in the grid
    /// @param gridRows number of rows in the grid
    /// @param gridColumns number of columns in the grid
    /// @param plotWidth width of each plot in characters
    /// @param plotHeight height of each plot in lines
    /// @param histogramCounts histogram bin counts per dimension
    /// @param globalMin minimum value across all dimensions
    /// @param globalMax maximum value across all dimensions
    /// @param samplesLoaded number of samples currently loaded
    /// @param targetSamples target number of samples to load
    public record GridState(
        String fileName,
        int vectorCount,
        int fileDimensions,
        int startDimension,
        int gridRows,
        int gridColumns,
        int plotWidth,
        int plotHeight,
        Map<Integer, int[]> histogramCounts,
        float globalMin,
        float globalMax,
        int samplesLoaded,
        int targetSamples
    ) {
        /// Creates a builder for GridState.
        public static Builder builder() {
            return new Builder();
        }

        /// Builder for GridState.
        public static class Builder {
            private String fileName = "unknown";
            private int vectorCount = 0;
            private int fileDimensions = 0;
            private int startDimension = 0;
            private int gridRows = 1;
            private int gridColumns = 1;
            private int plotWidth = 20;
            private int plotHeight = 10;
            private Map<Integer, int[]> histogramCounts = new HashMap<>();
            private float globalMin = -1.0f;
            private float globalMax = 1.0f;
            private int samplesLoaded = 0;
            private int targetSamples = 0;

            public Builder fileName(String fileName) { this.fileName = fileName; return this; }
            public Builder vectorCount(int vectorCount) { this.vectorCount = vectorCount; return this; }
            public Builder fileDimensions(int fileDimensions) { this.fileDimensions = fileDimensions; return this; }
            public Builder startDimension(int startDimension) { this.startDimension = startDimension; return this; }
            public Builder gridRows(int gridRows) { this.gridRows = gridRows; return this; }
            public Builder gridColumns(int gridColumns) { this.gridColumns = gridColumns; return this; }
            public Builder plotWidth(int plotWidth) { this.plotWidth = plotWidth; return this; }
            public Builder plotHeight(int plotHeight) { this.plotHeight = plotHeight; return this; }
            public Builder histogramCounts(Map<Integer, int[]> histogramCounts) { this.histogramCounts = histogramCounts; return this; }
            public Builder globalMin(float globalMin) { this.globalMin = globalMin; return this; }
            public Builder globalMax(float globalMax) { this.globalMax = globalMax; return this; }
            public Builder samplesLoaded(int samplesLoaded) { this.samplesLoaded = samplesLoaded; return this; }
            public Builder targetSamples(int targetSamples) { this.targetSamples = targetSamples; return this; }

            public GridState build() {
                return new GridState(fileName, vectorCount, fileDimensions, startDimension,
                    gridRows, gridColumns, plotWidth, plotHeight, histogramCounts,
                    globalMin, globalMax, samplesLoaded, targetSamples);
            }
        }
    }
}
