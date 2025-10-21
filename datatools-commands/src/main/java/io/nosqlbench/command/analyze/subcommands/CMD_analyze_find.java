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
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

/// Find a specific vector from one file in another file using lexicographic ordering
///
/// This command searches for a vector (specified by index) from a source file
/// in a target file. It first detects if the target file is sorted lexicographically
/// by checking the first and last 100 vectors. If sorted, it uses binary search;
/// otherwise it performs an exhaustive scan.
@CommandLine.Command(name = "find",
    header = "Find a vector in another file",
    description = "Searches for a specific vector from one file in another using lexicographic comparison, with binary search for sorted files or exhaustive scan otherwise",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:not found", "2:error"})
public class CMD_analyze_find implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_find.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_NOT_FOUND = 1;
    private static final int EXIT_ERROR = 2;

    private static final int ORDER_CHECK_SIZE = 100;
    private static final int TOP_MATCHES = 10;

    @CommandLine.Option(names = {"-s", "--source"}, description = "Source file containing the vector to find", required = true)
    private Path sourceFile;

    @CommandLine.Option(names = {"-t", "--target"}, description = "Target file to search in", required = true)
    private Path targetFile;

    @CommandLine.Option(names = {"-i", "--index"}, description = "Zero-based index of vector in source file", required = true)
    private int vectorIndex;

    @CommandLine.Option(names = {"--progress"}, description = "Show progress updates (default: auto-detect)")
    private Boolean showProgress;

    private volatile long vectorsScanned = 0;
    private volatile long totalVectors = 0;

    @Override
    public Integer call() throws Exception {
        try {
            // Validate files exist
            if (!Files.exists(sourceFile)) {
                System.err.println("Error: Source file not found: " + sourceFile);
                return EXIT_ERROR;
            }
            if (!Files.exists(targetFile)) {
                System.err.println("Error: Target file not found: " + targetFile);
                return EXIT_ERROR;
            }

            // Detect file types
            VectorFileExtension sourceExt = getVectorFileExtension(sourceFile);
            VectorFileExtension targetExt = getVectorFileExtension(targetFile);

            if (sourceExt == null || targetExt == null) {
                System.err.println("Error: Unsupported file type");
                return EXIT_ERROR;
            }

            // Verify compatible data types
            if (!sourceExt.getDataType().equals(targetExt.getDataType())) {
                System.err.println("Error: Source and target files have different data types");
                return EXIT_ERROR;
            }

            // Perform search based on data type
            Class<?> dataType = sourceExt.getDataType();
            if (dataType == float[].class) {
                return findVector(sourceExt.getFileType(), targetExt.getFileType(), float[].class);
            } else if (dataType == double[].class) {
                return findVector(sourceExt.getFileType(), targetExt.getFileType(), double[].class);
            } else if (dataType == int[].class) {
                return findVector(sourceExt.getFileType(), targetExt.getFileType(), int[].class);
            } else if (dataType == byte[].class) {
                return findVector(sourceExt.getFileType(), targetExt.getFileType(), byte[].class);
            } else {
                System.err.println("Error: Unsupported data type: " + dataType);
                return EXIT_ERROR;
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Error during find operation", e);
            return EXIT_ERROR;
        }
    }

    private <T> int findVector(FileType sourceType, FileType targetType, Class<T> dataClass) throws IOException {
        // 1. Read the target vector from source file
        printMessage("Reading vector " + vectorIndex + " from source file...");
        T targetVector = readVectorAtIndex(sourceFile, sourceType, dataClass, vectorIndex);
        if (targetVector == null) {
            System.err.println("Error: Vector index " + vectorIndex + " not found in source file");
            return EXIT_ERROR;
        }

        printMessage("Target vector: " + formatVectorAbbreviated(targetVector));

        // 2. Check if target file is sorted
        printMessage("Checking if target file is sorted...");
        boolean isSorted = isFileSorted(targetFile, targetType, dataClass);

        if (isSorted) {
            printMessage("Target file is sorted - using binary search");
            return binarySearchVector(targetFile, targetType, dataClass, targetVector);
        } else {
            printMessage("WARNING: Target file is NOT sorted - exhaustive search may be inefficient");
            return exhaustiveSearchVector(targetFile, targetType, dataClass, targetVector);
        }
    }

    /**
     * Read a vector at a specific index from a file using random access
     */
    private <T> T readVectorAtIndex(Path file, FileType fileType, Class<T> dataClass, int index) throws IOException {
        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, file);

        if (index >= reader.size()) {
            return null;
        }

        return reader.get(index);
    }

    /**
     * Check if a file is sorted by examining first and last 100 vectors using random access
     */
    private <T> boolean isFileSorted(Path file, FileType fileType, Class<T> dataClass) throws IOException {
        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, file);

        int totalSize = reader.size();
        if (totalSize < 2) {
            return true; // Trivially sorted
        }

        // Check first ORDER_CHECK_SIZE vectors
        int checkSize = Math.min(ORDER_CHECK_SIZE, totalSize);
        for (int i = 0; i < checkSize - 1; i++) {
            if (compareVectors(reader.get(i), reader.get(i + 1)) > 0) {
                return false;
            }
        }

        // Check last ORDER_CHECK_SIZE vectors
        int lastStart = Math.max(0, totalSize - ORDER_CHECK_SIZE);
        for (int i = lastStart; i < totalSize - 1; i++) {
            if (compareVectors(reader.get(i), reader.get(i + 1)) > 0) {
                return false;
            }
        }

        // Check if last of first set <= first of last set
        if (checkSize > 0 && lastStart < totalSize) {
            if (compareVectors(reader.get(checkSize - 1), reader.get(lastStart)) > 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Binary search for a vector in a sorted file using random access
     */
    private <T> int binarySearchVector(Path file, FileType fileType, Class<T> dataClass, T target) throws IOException {
        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, file);

        totalVectors = reader.size();

        printMessage("Performing binary search with random access...");
        int left = 0;
        int right = reader.size() - 1;
        int comparisons = 0;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            comparisons++;
            printProgress("Binary search", comparisons, (int)Math.ceil(Math.log(totalVectors) / Math.log(2)), "comparisons");

            int cmp = compareVectors(reader.get(mid), target);

            if (cmp == 0) {
                printProgress("Binary search", comparisons, comparisons, "comparisons");
                printMessage("");
                printMessage("Vector FOUND at index: " + mid + " (after " + comparisons + " comparisons)");
                return EXIT_SUCCESS;
            } else if (cmp < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // Not found - show neighbors
        printProgress("Binary search", comparisons, comparisons, "comparisons");
        printMessage("");
        printMessage("Vector NOT FOUND (after " + comparisons + " comparisons)");
        printMessage("Nearest vectors:");

        if (left > 0) {
            printMessage("  Before [" + (left - 1) + "]: " + formatVectorAbbreviated(reader.get(left - 1)));
        }
        if (left < reader.size()) {
            printMessage("  After  [" + left + "]: " + formatVectorAbbreviated(reader.get(left)));
        }

        return EXIT_NOT_FOUND;
    }

    /**
     * Exhaustive search using lexicographic comparison
     */
    private <T> int exhaustiveSearchVector(Path file, FileType fileType, Class<T> dataClass, T target) throws IOException {
        try (BoundedVectorFileStream<T> stream = VectorFileIO.streamIn(fileType, dataClass, file)
            .orElseThrow(() -> new RuntimeException("Could not open file: " + file))) {

            totalVectors = stream.getSize();
            vectorsScanned = 0;

            return exhaustiveSearchExact(stream, target);
        }
    }

    /**
     * Exhaustive search using exact lexicographic matching
     */
    private <T> int exhaustiveSearchExact(BoundedVectorFileStream<T> stream, T target) throws IOException {
        // Track top matches
        PriorityQueue<SimilarityMatch<T>> topMatches = new PriorityQueue<>(
            Comparator.comparingInt(m -> -m.matchingComponents));

        int targetDimension = getVectorDimension(target);
        printMessage("Starting exhaustive search with exact matching...");
        printMessage("Filtering by first component, then checking remaining components");

        int index = 0;
        for (T vector : stream) {
            vectorsScanned++;

            // Progressive component matching
            int matchingComponents = countMatchingComponents(vector, target);

            if (matchingComponents == targetDimension) {
                // Exact match found!
                printProgress("Scanning", (int)vectorsScanned, (int)totalVectors, "vectors");
                printMessage("");
                printMessage("Vector FOUND at index: " + index);
                printMessage("");
                printMessage("Top similar matches:");
                displayTopMatches(topMatches);
                return EXIT_SUCCESS;
            }

            // Track top similar matches
            if (matchingComponents > 0) {
                topMatches.offer(new SimilarityMatch<>(vector, index, matchingComponents));
                if (topMatches.size() > TOP_MATCHES) {
                    topMatches.poll(); // Remove least similar
                }
            }

            // Update progress every 1000 vectors
            if (vectorsScanned % 1000 == 0) {
                printProgress("Scanning", (int)vectorsScanned, (int)totalVectors,
                    String.format("vectors (best: %d/%d components)",
                        topMatches.isEmpty() ? 0 : topMatches.peek().matchingComponents,
                        targetDimension));
            }

            index++;
        }

        // Not found
        printProgress("Scanning", (int)vectorsScanned, (int)totalVectors, "vectors");
        printMessage("");
        printMessage("Vector NOT FOUND after exhaustive search");
        printMessage("");
        printMessage("Top " + Math.min(TOP_MATCHES, topMatches.size()) + " most similar matches:");
        displayTopMatches(topMatches);

        return EXIT_NOT_FOUND;
    }

    /**
     * Count how many components match between two vectors (from start)
     */
    private <T> int countMatchingComponents(T v1, T v2) {
        if (v1 instanceof float[]) {
            float[] a = (float[]) v1;
            float[] b = (float[]) v2;
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                if (a[i] != b[i]) return i;
            }
            return Math.min(a.length, b.length);
        } else if (v1 instanceof double[]) {
            double[] a = (double[]) v1;
            double[] b = (double[]) v2;
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                if (a[i] != b[i]) return i;
            }
            return Math.min(a.length, b.length);
        } else if (v1 instanceof int[]) {
            int[] a = (int[]) v1;
            int[] b = (int[]) v2;
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                if (a[i] != b[i]) return i;
            }
            return Math.min(a.length, b.length);
        } else if (v1 instanceof byte[]) {
            byte[] a = (byte[]) v1;
            byte[] b = (byte[]) v2;
            for (int i = 0; i < Math.min(a.length, b.length); i++) {
                if (a[i] != b[i]) return i;
            }
            return Math.min(a.length, b.length);
        }
        return 0;
    }

    /**
     * Compare two vectors lexicographically
     */
    private <T> int compareVectors(T v1, T v2) {
        if (v1 instanceof float[]) {
            return Arrays.compare((float[]) v1, (float[]) v2);
        } else if (v1 instanceof double[]) {
            return Arrays.compare((double[]) v1, (double[]) v2);
        } else if (v1 instanceof int[]) {
            return Arrays.compare((int[]) v1, (int[]) v2);
        } else if (v1 instanceof byte[]) {
            byte[] a = (byte[]) v1;
            byte[] b = (byte[]) v2;
            int len = Math.min(a.length, b.length);
            for (int i = 0; i < len; i++) {
                int cmp = Integer.compare(a[i] & 0xFF, b[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        }
        return 0;
    }

    /**
     * Get dimension of a vector
     */
    private <T> int getVectorDimension(T vector) {
        if (vector instanceof float[]) return ((float[]) vector).length;
        if (vector instanceof double[]) return ((double[]) vector).length;
        if (vector instanceof int[]) return ((int[]) vector).length;
        if (vector instanceof byte[]) return ((byte[]) vector).length;
        return 0;
    }

    /**
     * Format vector with abbreviation for display
     */
    private <T> String formatVectorAbbreviated(T vector) {
        if (vector instanceof float[]) {
            float[] v = (float[]) vector;
            if (v.length <= 5) return Arrays.toString(v);
            return String.format("[%f, %f, %f, ... %f, %f] (dim=%d)",
                v[0], v[1], v[2], v[v.length-2], v[v.length-1], v.length);
        } else if (vector instanceof double[]) {
            double[] v = (double[]) vector;
            if (v.length <= 5) return Arrays.toString(v);
            return String.format("[%f, %f, %f, ... %f, %f] (dim=%d)",
                v[0], v[1], v[2], v[v.length-2], v[v.length-1], v.length);
        } else if (vector instanceof int[]) {
            int[] v = (int[]) vector;
            if (v.length <= 5) return Arrays.toString(v);
            return String.format("[%d, %d, %d, ... %d, %d] (dim=%d)",
                v[0], v[1], v[2], v[v.length-2], v[v.length-1], v.length);
        } else if (vector instanceof byte[]) {
            byte[] v = (byte[]) vector;
            if (v.length <= 5) return Arrays.toString(v);
            return String.format("[%d, %d, %d, ... %d, %d] (dim=%d)",
                v[0], v[1], v[2], v[v.length-2], v[v.length-1], v.length);
        }
        return vector.toString();
    }

    /**
     * Display top similar matches
     */
    private <T> void displayTopMatches(PriorityQueue<SimilarityMatch<T>> matches) {
        List<SimilarityMatch<T>> sorted = new ArrayList<>(matches);

        // Sort by matching components (descending)
        sorted.sort(Comparator.comparingInt((SimilarityMatch<T> m) -> -m.matchingComponents));

        for (SimilarityMatch<T> match : sorted) {
            System.out.printf("  [%6d] %d/%d components: %s%n",
                match.index,
                match.matchingComponents,
                getVectorDimension(match.vector),
                formatVectorAbbreviated(match.vector));
        }
    }

    /**
     * Get file extension for a path
     */
    private VectorFileExtension getVectorFileExtension(Path file) {
        String fileName = file.getFileName().toString().toLowerCase();
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot < 0) return null;
        String extension = fileName.substring(lastDot);
        return VectorFileExtension.fromExtension(extension);
    }

    /**
     * Check if progress should be shown
     */
    private boolean shouldShowProgress() {
        if (showProgress != null) {
            return showProgress;
        }
        return System.console() != null;
    }

    /**
     * Print progress (thread-safe)
     */
    private synchronized void printProgress(String phase, int current, int total, String details) {
        if (!shouldShowProgress()) {
            return;
        }

        int percentage = total > 0 ? (current * 100 / total) : 0;
        String progress = String.format("\r%s: [%3d%%] %d/%d %s",
            phase, percentage, current, total, details);
        System.out.print(progress);
        System.out.flush();

        if (current >= total) {
            System.out.println();
        }
    }

    /**
     * Print message
     */
    private synchronized void printMessage(String message) {
        if (shouldShowProgress()) {
            System.out.print("\r" + " ".repeat(80) + "\r");
        }
        System.out.println(message);
    }

    /**
     * Holder for similar match information
     */
    private static class SimilarityMatch<T> {
        final T vector;
        final int index;
        final int matchingComponents;

        SimilarityMatch(T vector, int index, int matchingComponents) {
            this.vector = vector;
            this.index = index;
            this.matchingComponents = matchingComponents;
        }
    }
}
