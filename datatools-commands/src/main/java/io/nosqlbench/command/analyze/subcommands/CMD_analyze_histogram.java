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

import io.nosqlbench.vshapes.extract.DimensionStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Display ASCII histogram for a specific dimension in a vector file.
///
/// This command reads a dimension from a vector file and displays its
/// distribution as an ASCII histogram, useful for quick visual inspection
/// of data characteristics.
///
/// ## Usage
///
/// ```bash
/// nbvectors analyze histogram --input data.fvec --dimension 23
/// nbvectors analyze histogram --input data.fvec --dimension 23 --bins 50
/// ```
@CommandLine.Command(
    name = "histogram",
    header = "Display ASCII histogram for a dimension",
    description = "Shows the distribution of values in a specific dimension as an ASCII histogram.",
    exitCodeList = {
        "0: Success",
        "1: Error reading file"
    }
)
public class CMD_analyze_histogram implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_histogram.class);

    // Unicode block characters for histogram bars
    private static final String[] BLOCKS = {"▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"};

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_BLUE = "\u001B[94m";
    private static final String ANSI_CYAN = "\u001B[96m";
    private static final String ANSI_GREEN = "\u001B[92m";

    @CommandLine.Option(
        names = {"--input", "-i"},
        description = "Path to the vector file (.fvec format)",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"--dimension", "-d"},
        description = "Dimension to visualize (0-indexed)",
        required = true
    )
    private int dimension;

    @CommandLine.Option(
        names = {"--bins", "-b"},
        description = "Number of histogram bins (default: 40)"
    )
    private int bins = 40;

    @CommandLine.Option(
        names = {"--width", "-w"},
        description = "Width of histogram bars in characters (default: 60)"
    )
    private int width = 60;

    @CommandLine.Option(
        names = {"--sample", "-s"},
        description = "Maximum number of vectors to sample (default: all)"
    )
    private Integer sampleSize;

    @CommandLine.Option(
        names = {"--vertical"},
        description = "Display vertical histogram (sparkline style)"
    )
    private boolean vertical = false;

    @Override
    public Integer call() {
        try {
            if (!Files.exists(inputPath)) {
                System.err.println("Error: File not found: " + inputPath);
                return 1;
            }

            // Read file header
            int[] fileInfo = readFvecHeader(inputPath);
            int dimensions = fileInfo[0];
            int vectorCount = fileInfo[1];

            if (dimension < 0 || dimension >= dimensions) {
                System.err.printf("Error: Dimension %d out of range [0, %d)%n", dimension, dimensions);
                return 1;
            }

            int samplesToUse = sampleSize != null ? Math.min(sampleSize, vectorCount) : vectorCount;

            // Read dimension values
            float[] values = readDimensionValues(dimension, dimensions, samplesToUse);
            DimensionStatistics stats = DimensionStatistics.compute(dimension, values);

            // Print header
            System.out.println();
            System.out.printf("%sDimension %d Histogram%s%n", ANSI_BOLD, dimension, ANSI_RESET);
            System.out.println("═══════════════════════════════════════════════════════════════════════════════");
            System.out.printf("File: %s%n", inputPath);
            System.out.printf("Samples: %,d%s%n", samplesToUse,
                sampleSize != null ? String.format(" of %,d", vectorCount) : "");
            System.out.printf("Range: [%.6f, %.6f]%n", stats.min(), stats.max());
            System.out.printf("Mean: %.6f, StdDev: %.6f%n", stats.mean(), stats.stdDev());
            System.out.println();

            if (vertical) {
                printVerticalHistogram(values, stats);
            } else {
                printHorizontalHistogram(values, stats);
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error creating histogram", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private void printHorizontalHistogram(float[] values, DimensionStatistics stats) {
        double min = stats.min();
        double max = stats.max();
        double binWidth = (max - min) / bins;

        // Count values in each bin
        int[] counts = new int[bins];
        for (float v : values) {
            int bin = (int) ((v - min) / binWidth);
            if (bin >= bins) bin = bins - 1;
            if (bin < 0) bin = 0;
            counts[bin]++;
        }

        // Find max count for scaling
        int maxCount = 0;
        for (int c : counts) {
            if (c > maxCount) maxCount = c;
        }

        // Print histogram
        for (int i = 0; i < bins; i++) {
            double binStart = min + i * binWidth;
            double binEnd = binStart + binWidth;

            // Calculate bar length
            int barLength = maxCount > 0 ? (int) Math.round((double) counts[i] / maxCount * width) : 0;

            // Build bar using Unicode blocks for sub-character precision
            StringBuilder bar = new StringBuilder();
            int fullBlocks = barLength;
            for (int j = 0; j < fullBlocks; j++) {
                bar.append(ANSI_GREEN).append("█").append(ANSI_RESET);
            }

            // Print row
            System.out.printf("%s%10.4f%s │%s %s%,d%n",
                ANSI_DIM, binStart, ANSI_RESET,
                bar,
                ANSI_DIM, counts[i]);
        }

        // Print axis line
        System.out.print("           └");
        for (int i = 0; i < width + 1; i++) {
            System.out.print("─");
        }
        System.out.println();
    }

    private void printVerticalHistogram(float[] values, DimensionStatistics stats) {
        double min = stats.min();
        double max = stats.max();
        double binWidth = (max - min) / bins;

        // Count values in each bin
        int[] counts = new int[bins];
        for (float v : values) {
            int bin = (int) ((v - min) / binWidth);
            if (bin >= bins) bin = bins - 1;
            if (bin < 0) bin = 0;
            counts[bin]++;
        }

        // Find max count for scaling
        int maxCount = 0;
        for (int c : counts) {
            if (c > maxCount) maxCount = c;
        }

        // Normalize to 8 levels (for Unicode block characters)
        int height = 8;
        System.out.print("  ");
        for (int i = 0; i < bins; i++) {
            int level = maxCount > 0 ? (int) Math.round((double) counts[i] / maxCount * height) : 0;
            level = Math.min(level, 8);
            if (level == 0) {
                System.out.print(" ");
            } else {
                System.out.print(ANSI_GREEN + BLOCKS[level - 1] + ANSI_RESET);
            }
        }
        System.out.println();

        // Print axis
        System.out.printf("  %s%.2f%s", ANSI_DIM, min, ANSI_RESET);
        int padding = bins - 10;
        for (int i = 0; i < padding; i++) System.out.print(" ");
        System.out.printf("%s%.2f%s%n", ANSI_DIM, max, ANSI_RESET);

        // Print sparkline summary
        System.out.println();
        System.out.print(ANSI_CYAN + "Sparkline: " + ANSI_RESET);
        for (int i = 0; i < bins; i++) {
            int level = maxCount > 0 ? (int) Math.round((double) counts[i] / maxCount * 8) : 0;
            level = Math.min(level, 8);
            if (level == 0) {
                System.out.print("▁");
            } else {
                System.out.print(BLOCKS[level - 1]);
            }
        }
        System.out.println();
    }

    private int[] readFvecHeader(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            byte[] dimBytes = new byte[4];
            raf.read(dimBytes);
            int dimensions = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            long fileSize = raf.length();
            int bytesPerVector = 4 + dimensions * 4;
            int vectorCount = (int) (fileSize / bytesPerVector);

            return new int[]{dimensions, vectorCount};
        }
    }

    private float[] readDimensionValues(int dim, int dimensions, int vectorCount) throws IOException {
        float[] values = new float[vectorCount];
        int bytesPerVector = 4 + dimensions * 4;

        try (RandomAccessFile raf = new RandomAccessFile(inputPath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

            for (int v = 0; v < vectorCount; v++) {
                long offset = (long) v * bytesPerVector + 4 + (long) dim * 4;
                channel.position(offset);
                buffer.clear();
                channel.read(buffer);
                buffer.flip();
                values[v] = buffer.getFloat();
            }
        }

        return values;
    }
}
