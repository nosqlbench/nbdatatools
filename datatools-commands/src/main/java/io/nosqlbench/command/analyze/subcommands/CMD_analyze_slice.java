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
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.command.common.VectorDataCompletionCandidates;
import io.nosqlbench.command.common.VectorDataSpec;
import io.nosqlbench.command.common.VectorDataSpecConverter;
import io.nosqlbench.command.common.VectorDataSpecSupport;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.merklev2.CacheFileAccessor;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.CoreXVecVectorDatasetViewMethods;
import io.nosqlbench.vectordata.spec.datasets.types.VectorDatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.Callable;

/// Slice vectors by ordinal and component ranges
///
/// This command extracts a subset of vectors and/or dimensions from a vector file.
/// Supports flexible range syntax: closed intervals [m,n], open intervals (m,n),
/// and mixed [m,n), (m,n].
@CommandLine.Command(name = "slice",
    header = "Slice vectors by ordinal and component ranges",
    description = "Extract subsets of vectors and dimensions with flexible range syntax",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:error"})
public class CMD_analyze_slice implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_slice.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_ERROR = 1;

    // Maximum memory per chunk: 1GB
    private static final long MAX_CHUNK_MEMORY = 1024L * 1024L * 1024L;

    @CommandLine.Parameters(index = "0", description = "Input vector data source",
        converter = VectorDataSpecConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class)
    private VectorDataSpec vectors;

    @CommandLine.Option(names = {"--ordinal-range"}, description = "Ordinal range: n, m..n, [m,n), (m,n], etc.")
    private String ordinalRange;

    @CommandLine.Option(names = {"--component-range"}, description = "Component range: n, m..n, [m,n), (m,n], etc.")
    private String componentRange;

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--cache-dir"},
        description = "Directory for cached dataset files")
    private Path cacheDir;

    @CommandLine.Option(names = {"--format"}, description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", defaultValue = "TEXT")
    private OutputFormat format = OutputFormat.TEXT;

    @CommandLine.Option(names = {"--max-vectors"}, description = "Maximum vectors to display in text format (default: 100)")
    private int maxVectors = 100;

    private Path inputPath;
    private long ordinalStart = 0;
    private long ordinalEnd = Long.MAX_VALUE;
    private int componentStart = 0;
    private int componentEnd = Integer.MAX_VALUE;

    enum OutputFormat {
        TEXT,       // Human-readable text
        CSV,        // Comma-separated values
        TSV,        // Tab-separated values
        JSON        // JSON array
    }

    @Override
    public Integer call() throws Exception {
        try {
            this.configdir = VectorDataSpecSupport.expandPath(this.configdir);
            inputPath = resolveInputFile();
            parseRanges();

            // Detect file type
            VectorFileExtension ext = getVectorFileExtension(inputPath);
            if (ext == null) {
                System.err.println("Error: Unsupported file type");
                return EXIT_ERROR;
            }

            // Perform the slice operation
            Class<?> dataType = ext.getDataType();
            if (dataType == float[].class) {
                return sliceVectors(ext.getFileType(), float[].class);
            } else if (dataType == double[].class) {
                return sliceVectors(ext.getFileType(), double[].class);
            } else if (dataType == int[].class) {
                return sliceVectors(ext.getFileType(), int[].class);
            } else if (dataType == byte[].class) {
                return sliceVectors(ext.getFileType(), byte[].class);
            } else {
                System.err.println("Error: Unsupported data type: " + dataType);
                return EXIT_ERROR;
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Error during slice operation", e);
            return EXIT_ERROR;
        }
    }

    private void parseRanges() {
        RangeOption.RangeConverter converter = new RangeOption.RangeConverter();

        if (ordinalRange != null && !ordinalRange.trim().isEmpty()) {
            RangeOption.Range range = converter.convert(ordinalRange);
            ordinalStart = range.start();
            ordinalEnd = range.end();
        }

        if (componentRange != null && !componentRange.trim().isEmpty()) {
            RangeOption.Range range = converter.convert(componentRange);
            componentStart = (int) range.start();
            componentEnd = (int) range.end();
        }
    }

    private Path resolveInputFile() throws Exception {
        if (vectors.isLocalFile()) {
            Path file = vectors.getLocalPath().orElseThrow();
            if (!Files.exists(file)) {
                throw new IllegalArgumentException("Input file not found: " + file);
            }
            return file;
        }
        if (!vectors.isFacet()) {
            throw new IllegalArgumentException("Unsupported vector data source: " + vectors);
        }

        TestDataKind facetKind = vectors.getFacetKind().orElseThrow();
        VectorDatasetView<?> view = VectorDataSpecSupport
            .resolveDatasetView(vectors, configdir, catalogs, cacheDir)
            .orElseThrow(() -> new IllegalArgumentException(
                "Facet '" + facetKind.name() + "' is not available for " + vectors));

        if (!(view instanceof CoreXVecVectorDatasetViewMethods<?> xvecView)) {
            throw new IllegalArgumentException("Facet '" + facetKind.name() + "' is not backed by an xvec file.");
        }
        if (!(xvecView.getChannel() instanceof CacheFileAccessor cacheAccessor)) {
            throw new IllegalStateException("Facet '" + facetKind.name() + "' does not expose a cache file path.");
        }

        view.prebuffer().get();
        return cacheAccessor.getCacheFilePath();
    }

    /**
     * Slice vectors with the specified ranges
     */
    private <T> int sliceVectors(FileType fileType, Class<T> dataClass) throws IOException {
        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, inputPath);

        // Constrain ranges to actual file bounds
        long actualStart = Math.max(0, ordinalStart);
        long actualEnd = Math.min(ordinalEnd, reader.size());

        if (actualStart >= reader.size()) {
            System.err.println("Error: Ordinal range start " + ordinalStart + " is beyond file size " + reader.size());
            return EXIT_ERROR;
        }

        long totalVectors = actualEnd - actualStart;

        // Get vector dimension from first vector
        T firstVector = reader.get((int) actualStart);
        int vectorDim = getVectorDimension(firstVector);

        // Constrain component range
        int actualComponentStart = Math.max(0, componentStart);
        int actualComponentEnd = Math.min(componentEnd, vectorDim);

        if (actualComponentStart >= vectorDim) {
            System.err.println("Error: Component range start " + componentStart + " is beyond vector dimension " + vectorDim);
            return EXIT_ERROR;
        }

        // Print header information
        printHeader(totalVectors, actualStart, actualEnd, actualComponentStart, actualComponentEnd, vectorDim);

        // Calculate chunk size based on memory constraints
        int bytesPerElement = getBytesPerElement(dataClass);
        int componentsPerVector = actualComponentEnd - actualComponentStart;
        long bytesPerVector = componentsPerVector * bytesPerElement;
        long maxVectorsPerChunk = Math.max(1, MAX_CHUNK_MEMORY / bytesPerVector);

        // Process in chunks
        long processed = 0;
        for (long chunkStart = actualStart; chunkStart < actualEnd; chunkStart += maxVectorsPerChunk) {
            long chunkEnd = Math.min(chunkStart + maxVectorsPerChunk, actualEnd);
            int chunkSize = (int) (chunkEnd - chunkStart);

            // For text format, respect maxVectors limit
            if (format == OutputFormat.TEXT && processed >= maxVectors) {
                System.out.println("... (" + (totalVectors - processed) + " more vectors)");
                break;
            }

            processChunk(reader, (int) chunkStart, (int) chunkEnd, actualComponentStart, actualComponentEnd, processed);
            processed += chunkSize;
        }

        printFooter();
        return EXIT_SUCCESS;
    }

    /**
     * Process a chunk of vectors
     */
    private <T> void processChunk(VectorFileArray<T> reader, int start, int end,
                                    int compStart, int compEnd, long offset) {
        for (int i = start; i < end; i++) {
            // For text format, respect maxVectors limit
            if (format == OutputFormat.TEXT && (i - start + offset) >= maxVectors) {
                break;
            }

            T vector = reader.get(i);
            T sliced = sliceComponents(vector, compStart, compEnd);
            printVector(i, sliced);
        }
    }

    /**
     * Slice components from a vector
     */
    @SuppressWarnings("unchecked")
    private <T> T sliceComponents(T vector, int start, int end) {
        if (vector instanceof float[]) {
            float[] v = (float[]) vector;
            return (T) Arrays.copyOfRange(v, start, end);
        } else if (vector instanceof double[]) {
            double[] v = (double[]) vector;
            return (T) Arrays.copyOfRange(v, start, end);
        } else if (vector instanceof int[]) {
            int[] v = (int[]) vector;
            return (T) Arrays.copyOfRange(v, start, end);
        } else if (vector instanceof byte[]) {
            byte[] v = (byte[]) vector;
            return (T) Arrays.copyOfRange(v, start, end);
        }
        throw new IllegalArgumentException("Unsupported vector type: " + vector.getClass());
    }

    /**
     * Get the dimension of a vector
     */
    private <T> int getVectorDimension(T vector) {
        if (vector instanceof float[]) return ((float[]) vector).length;
        if (vector instanceof double[]) return ((double[]) vector).length;
        if (vector instanceof int[]) return ((int[]) vector).length;
        if (vector instanceof byte[]) return ((byte[]) vector).length;
        return 0;
    }

    /**
     * Get bytes per element for a data type
     */
    private <T> int getBytesPerElement(Class<T> dataClass) {
        if (dataClass == double[].class) return 8;
        if (dataClass == float[].class) return 4;
        if (dataClass == int[].class) return 4;
        if (dataClass == byte[].class) return 1;
        return 4;
    }

    /**
     * Print header based on format
     */
    private void printHeader(long totalVectors, long start, long end, int compStart, int compEnd, int vectorDim) {
        switch (format) {
            case TEXT:
                System.out.println("Slicing vectors [" + start + ", " + end + ") with components [" + compStart + ", " + compEnd + ")");
                System.out.println("Total: " + totalVectors + " vectors, " + (compEnd - compStart) + " dimensions (original: " + vectorDim + ")");
                System.out.println();
                break;
            case JSON:
                System.out.println("[");
                break;
            case CSV:
            case TSV:
                // No header for CSV/TSV
                break;
        }
    }

    /**
     * Print a vector based on format
     */
    private <T> void printVector(int index, T vector) {
        switch (format) {
            case TEXT:
                System.out.println("[" + index + "] " + formatVector(vector));
                break;
            case JSON:
                System.out.println("  {\"index\": " + index + ", \"vector\": " + formatVectorJson(vector) + "},");
                break;
            case CSV:
                System.out.println(index + "," + formatVectorCsv(vector));
                break;
            case TSV:
                System.out.println(index + "\t" + formatVectorTsv(vector));
                break;
        }
    }

    /**
     * Print footer based on format
     */
    private void printFooter() {
        switch (format) {
            case JSON:
                System.out.println("]");
                break;
            default:
                // No footer for other formats
                break;
        }
    }

    /**
     * Format vector for text display
     */
    private <T> String formatVector(T vector) {
        if (vector instanceof float[]) {
            return Arrays.toString((float[]) vector);
        } else if (vector instanceof double[]) {
            return Arrays.toString((double[]) vector);
        } else if (vector instanceof int[]) {
            return Arrays.toString((int[]) vector);
        } else if (vector instanceof byte[]) {
            return Arrays.toString((byte[]) vector);
        }
        return vector.toString();
    }

    /**
     * Format vector as JSON array
     */
    private <T> String formatVectorJson(T vector) {
        StringBuilder sb = new StringBuilder("[");
        if (vector instanceof float[]) {
            float[] v = (float[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(v[i]);
            }
        } else if (vector instanceof double[]) {
            double[] v = (double[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(v[i]);
            }
        } else if (vector instanceof int[]) {
            int[] v = (int[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(v[i]);
            }
        } else if (vector instanceof byte[]) {
            byte[] v = (byte[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(v[i] & 0xFF);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Format vector as CSV
     */
    private <T> String formatVectorCsv(T vector) {
        StringBuilder sb = new StringBuilder();
        if (vector instanceof float[]) {
            float[] v = (float[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(v[i]);
            }
        } else if (vector instanceof double[]) {
            double[] v = (double[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(v[i]);
            }
        } else if (vector instanceof int[]) {
            int[] v = (int[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(v[i]);
            }
        } else if (vector instanceof byte[]) {
            byte[] v = (byte[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(v[i] & 0xFF);
            }
        }
        return sb.toString();
    }

    /**
     * Format vector as TSV
     */
    private <T> String formatVectorTsv(T vector) {
        StringBuilder sb = new StringBuilder();
        if (vector instanceof float[]) {
            float[] v = (float[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append("\t");
                sb.append(v[i]);
            }
        } else if (vector instanceof double[]) {
            double[] v = (double[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append("\t");
                sb.append(v[i]);
            }
        } else if (vector instanceof int[]) {
            int[] v = (int[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append("\t");
                sb.append(v[i]);
            }
        } else if (vector instanceof byte[]) {
            byte[] v = (byte[]) vector;
            for (int i = 0; i < v.length; i++) {
                if (i > 0) sb.append("\t");
                sb.append(v[i] & 0xFF);
            }
        }
        return sb.toString();
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
}
