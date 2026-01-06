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

/**
 * Formats fit quality data as an aligned ASCII table.
 *
 * <p>This class is separate from {@link DimensionFitReport} to enable
 * independent testing of the formatting logic.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * FitTableFormatter formatter = new FitTableFormatter(modelTypes);
 * for (int d = 0; d < dimensions; d++) {
 *     formatter.addRow(d, scores[d], bestFitIndex);
 * }
 * String table = formatter.format();
 * }</pre>
 */
public final class FitTableFormatter {

    private static final String INFINITY = "---";
    private static final String NA = "N/A";
    private static final int MIN_SCORE_WIDTH = 7;
    private static final int MIN_DIM_WIDTH = 3;
    private static final int BEST_COL_WIDTH = 12;
    private static final int SPARKLINE_WIDTH = 12;

    private final List<String> columnHeaders;
    private final List<RowData> rows;
    private final boolean showSparklines;

    /**
     * Creates a formatter with the specified column headers (model types).
     *
     * @param columnHeaders list of model type names for column headers
     */
    public FitTableFormatter(List<String> columnHeaders) {
        this(columnHeaders, false);
    }

    /**
     * Creates a formatter with the specified column headers and sparkline option.
     *
     * @param columnHeaders list of model type names for column headers
     * @param showSparklines whether to include sparkline column
     */
    public FitTableFormatter(List<String> columnHeaders, boolean showSparklines) {
        this.columnHeaders = new ArrayList<>(columnHeaders);
        this.rows = new ArrayList<>();
        this.showSparklines = showSparklines;
    }

    /**
     * Adds a row of fit scores for a dimension.
     *
     * @param dimension the dimension index
     * @param scores array of fit scores (one per column)
     * @param bestIndex index of the best (lowest) score, or -1 if none
     */
    public void addRow(int dimension, double[] scores, int bestIndex) {
        addRow(dimension, scores, bestIndex, null);
    }

    /**
     * Adds a row of fit scores for a dimension with optional sparkline.
     *
     * @param dimension the dimension index
     * @param scores array of fit scores (one per column)
     * @param bestIndex index of the best (lowest) score, or -1 if none
     * @param sparkline Unicode sparkline histogram (may be null)
     */
    public void addRow(int dimension, double[] scores, int bestIndex, String sparkline) {
        if (scores.length != columnHeaders.size()) {
            throw new IllegalArgumentException(
                "Score count (" + scores.length + ") must match column count (" + columnHeaders.size() + ")");
        }
        rows.add(new RowData(dimension, scores.clone(), bestIndex, sparkline != null ? sparkline : ""));
    }

    /**
     * Formats the table as an aligned string.
     *
     * @return formatted table string
     */
    public String format() {
        if (rows.isEmpty()) {
            return "No data\n";
        }

        int[] colWidths = computeColumnWidths();
        int dimWidth = computeDimWidth();

        StringBuilder sb = new StringBuilder();
        appendHeader(sb, dimWidth, colWidths);
        appendSeparator(sb, dimWidth, colWidths);
        appendDataRows(sb, dimWidth, colWidths);

        return sb.toString();
    }

    /**
     * Formats a score value to a consistent width string.
     *
     * @param score the score value
     * @param width the target width
     * @param isBest true if this is the best score (add marker)
     * @return formatted score string of exact width
     */
    static String formatScore(double score, int width, boolean isBest) {
        String marker = isBest ? "*" : " ";
        int valueWidth = width - 1; // Reserve 1 char for marker

        String valueStr;
        if (Double.isNaN(score) || Double.isInfinite(score)) {
            valueStr = NA;
        } else if (score >= 1e10) {
            valueStr = INFINITY;
        } else if (score >= 10000) {
            valueStr = String.format("%.0f", score);
        } else if (score >= 1000) {
            valueStr = String.format("%.0f", score);
        } else if (score >= 100) {
            valueStr = String.format("%.1f", score);
        } else if (score >= 10) {
            valueStr = String.format("%.2f", score);
        } else {
            valueStr = String.format("%.3f", score);
        }

        // Right-pad or truncate to fit valueWidth
        if (valueStr.length() > valueWidth) {
            valueStr = valueStr.substring(0, valueWidth);
        } else if (valueStr.length() < valueWidth) {
            valueStr = pad(valueStr, valueWidth, true);
        }

        return valueStr + marker;
    }

    /**
     * Pads a string to the specified width.
     *
     * @param s the string to pad
     * @param width target width
     * @param rightAlign true for right alignment, false for left
     * @return padded string
     */
    static String pad(String s, int width, boolean rightAlign) {
        if (s.length() >= width) {
            return s;
        }
        int padding = width - s.length();
        if (rightAlign) {
            return " ".repeat(padding) + s;
        } else {
            return s + " ".repeat(padding);
        }
    }

    /**
     * Center-pads a string to the specified width.
     *
     * @param s the string to center
     * @param width target width
     * @return center-padded string
     */
    static String centerPad(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        int padding = width - s.length();
        int left = padding / 2;
        int right = padding - left;
        return " ".repeat(left) + s + " ".repeat(right);
    }

    private int[] computeColumnWidths() {
        int[] widths = new int[columnHeaders.size()];
        for (int i = 0; i < columnHeaders.size(); i++) {
            // Width must fit header + 1 for asterisk marker
            widths[i] = Math.max(columnHeaders.get(i).length() + 1, MIN_SCORE_WIDTH);
        }
        return widths;
    }

    private int computeDimWidth() {
        int maxDim = rows.isEmpty() ? 0 : rows.get(rows.size() - 1).dimension;
        return Math.max(MIN_DIM_WIDTH, String.valueOf(maxDim).length());
    }

    private void appendHeader(StringBuilder sb, int dimWidth, int[] colWidths) {
        sb.append(pad("Dim", dimWidth, true));
        if (showSparklines) {
            sb.append(" | ").append(centerPad("Distribution", SPARKLINE_WIDTH));
        }
        for (int i = 0; i < columnHeaders.size(); i++) {
            sb.append(" | ").append(centerPad(columnHeaders.get(i), colWidths[i]));
        }
        sb.append(" | ").append(centerPad("Best", BEST_COL_WIDTH));
        sb.append("\n");
    }

    private void appendSeparator(StringBuilder sb, int dimWidth, int[] colWidths) {
        sb.append("-".repeat(dimWidth));
        if (showSparklines) {
            sb.append("-+-").append("-".repeat(SPARKLINE_WIDTH));
        }
        for (int colWidth : colWidths) {
            sb.append("-+-").append("-".repeat(colWidth));
        }
        sb.append("-+-").append("-".repeat(BEST_COL_WIDTH));
        sb.append("\n");
    }

    private void appendDataRows(StringBuilder sb, int dimWidth, int[] colWidths) {
        for (RowData row : rows) {
            sb.append(pad(String.valueOf(row.dimension), dimWidth, true));

            if (showSparklines) {
                String spark = row.sparkline.isEmpty()
                    ? " ".repeat(SPARKLINE_WIDTH)
                    : pad(row.sparkline, SPARKLINE_WIDTH, false);
                sb.append(" | ").append(spark);
            }

            for (int i = 0; i < row.scores.length; i++) {
                boolean isBest = (i == row.bestIndex);
                String scoreStr = formatScore(row.scores[i], colWidths[i], isBest);
                sb.append(" | ").append(scoreStr);
            }

            String bestType = (row.bestIndex >= 0 && row.bestIndex < columnHeaders.size())
                ? columnHeaders.get(row.bestIndex)
                : "unknown";
            sb.append(" | ").append(pad(bestType, BEST_COL_WIDTH, false));
            sb.append("\n");
        }
    }

    private record RowData(int dimension, double[] scores, int bestIndex, String sparkline) {}
}
