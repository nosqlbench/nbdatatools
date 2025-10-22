package io.nosqlbench.command.compute.subcommands;

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

import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.status.StatusContext;
import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.StatusTracker;
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSink;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.sinks.ConsoleLoggerSink;
import io.nosqlbench.status.sinks.ConsolePanelSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/// Command to sort vectors from xvec format files using memory-efficient external merge sort
///
/// This command sorts vectors lexicographically using an external merge sort algorithm
/// that is designed to work efficiently with limited memory. The algorithm:
///
/// 1. Reads vectors in chunks that fit in memory
/// 2. Sorts each chunk using American Flag Radix Sort (byte-by-byte MSD radix sort)
/// 3. Writes sorted chunks to temporary files
/// 4. Merges sorted chunks using a k-way merge with minimal memory footprint
///
/// The sorting is stable and handles all xvec formats (fvec, ivec, bvec, dvec).
/// NaN values in floating-point vectors are sorted to the end.
@CommandLine.Command(name = "sort",
    description = "Sort vectors from xvec format files using memory-efficient external merge sort")
public class CMD_compute_sort implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_compute_sort.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    // Default chunk size: 100K vectors (adjustable based on memory and vector dimension)
    private static final int DEFAULT_CHUNK_SIZE = 100_000;

    @CommandLine.Option(names = {"-i", "--input"}, description = "The input vectors file", required = true)
    private Path inputPath;

    @CommandLine.Option(names = {"-o", "--output"}, description = "The output sorted vectors file", required = true)
    private Path outputPath;

    @CommandLine.Option(names = {"-c", "--chunk-size"},
        description = "Number of vectors per chunk (default: ${DEFAULT-VALUE})",
        defaultValue = "" + DEFAULT_CHUNK_SIZE)
    private int chunkSize = DEFAULT_CHUNK_SIZE;

    @CommandLine.Option(names = {"-t", "--temp-dir"},
        description = "Directory for temporary chunk files (default: system temp)")
    private Path tempDir;

    @CommandLine.Option(names = {"-f", "--force"}, description = "Force overwrite if output file already exists")
    private boolean force = false;

    @CommandLine.Option(names = {"-p", "--parallel"},
        description = "Enable parallel sorting (auto-sizes based on available memory and CPU cores)")
    private boolean parallel = false;

    @CommandLine.Option(names = {"--threads"},
        description = "Number of parallel threads (default: auto-detect, always leaves 1 core free)")
    private Integer explicitThreads;

    @CommandLine.Option(names = {"--progress"},
        description = "Show progress updates (default: true for TTY, false otherwise)")
    private Boolean showProgress;

    @CommandLine.Option(names = {"--resume"},
        description = "Resume from existing chunk files (skip chunks that are already complete)")
    private boolean resume = false;

    @CommandLine.Option(names = {"--range"},
        description = "Range of vectors to sort. Formats: 'n' (0 to n-1), 'm..n' (m to n inclusive), '[m,n)' (m to n-1)")
    private String range = null;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    // Parsed range values
    private long rangeStart = 0;
    private long rangeEnd = Long.MAX_VALUE;
    private boolean rangeSpecified = false;

    private static final class ChunkSortProgress implements RunnableStatus<ChunkSortProgress> {
        private final AtomicInteger expectedChunks = new AtomicInteger();
        private final AtomicInteger completedChunks = new AtomicInteger();
        private final AtomicInteger resumedChunks = new AtomicInteger();
        private final AtomicLong processedVectors = new AtomicLong();
        private final AtomicLong totalVectors = new AtomicLong();
        private final AtomicReference<RunState> state = new AtomicReference<>(RunState.PENDING);

        void start(int chunks, long vectors) {
            expectedChunks.set(Math.max(chunks, 0));
            totalVectors.set(Math.max(vectors, 0));
            state.set(RunState.RUNNING);
        }

        void recordChunk(int vectors, boolean resumed) {
            long addedVectors = Math.max(vectors, 0);
            long total = totalVectors.get();
            long updated = processedVectors.addAndGet(addedVectors);
            if (total > 0 && updated > total) {
                processedVectors.set(total);
            }

            int newCompleted = completedChunks.incrementAndGet();
            int expected = expectedChunks.get();
            if (expected > 0 && newCompleted > expected) {
                completedChunks.set(expected);
            }
            if (resumed) {
                resumedChunks.incrementAndGet();
            }
        }

        void success() {
            int expected = expectedChunks.get();
            if (expected > 0) {
                completedChunks.set(expected);
            }
            long total = totalVectors.get();
            if (total > 0) {
                processedVectors.set(total);
            }
            state.set(RunState.SUCCESS);
        }

        void fail() {
            state.updateAndGet(current -> {
                if (current == RunState.SUCCESS || current == RunState.FAILED || current == RunState.CANCELLED) {
                    return current;
                }
                return RunState.FAILED;
            });
        }

        @Override
        public StatusUpdate<ChunkSortProgress> toStatusUpdate() {
            int expected = expectedChunks.get();
            int completed = completedChunks.get();
            long total = totalVectors.get();
            long processed = processedVectors.get();
            RunState currentState = state.get();

            double chunkFraction = expected > 0 ? Math.min(1.0, (double) completed / expected) : 0.0;
            double vectorFraction = total > 0 ? Math.min(1.0, (double) processed / total) : chunkFraction;
            double progress = expected > 0 && total > 0 ? (chunkFraction + vectorFraction) / 2.0
                : (expected > 0 ? chunkFraction : vectorFraction);

            if (currentState == RunState.SUCCESS) {
                progress = 1.0;
            }

            return new StatusUpdate<>(progress, currentState, this);
        }

        @Override
        public String getName() {
            int expected = expectedChunks.get();
            int completed = completedChunks.get();
            long total = totalVectors.get();
            long processed = processedVectors.get();
            int resumed = resumedChunks.get();

            String chunkPart = expected > 0
                ? String.format("%d/%d chunks", completed, expected)
                : String.format("%d chunks", completed);
            String vectorPart = total > 0
                ? String.format("%,d/%,d vectors", processed, total)
                : String.format("%,d vectors", processed);
            String resumePart = resumed > 0 ? ", resumed " + resumed : "";
            return "Chunk Sorting (" + chunkPart + ", " + vectorPart + resumePart + ")";
        }
    }

    private static final class MergeProgress implements RunnableStatus<MergeProgress> {
        private final AtomicLong totalVectors = new AtomicLong();
        private final AtomicLong mergedVectors = new AtomicLong();
        private final AtomicReference<RunState> state = new AtomicReference<>(RunState.PENDING);

        void start(long total) {
            totalVectors.set(Math.max(total, 0));
            state.set(RunState.RUNNING);
        }

        void updateMerged(long merged) {
            long capped = Math.max(merged, 0);
            long total = totalVectors.get();
            if (total > 0 && capped > total) {
                capped = total;
            }
            mergedVectors.set(capped);
        }

        void success() {
            long total = totalVectors.get();
            if (total > 0) {
                mergedVectors.set(total);
            }
            state.set(RunState.SUCCESS);
        }

        void fail() {
            state.updateAndGet(current -> {
                if (current == RunState.SUCCESS || current == RunState.FAILED || current == RunState.CANCELLED) {
                    return current;
                }
                return RunState.FAILED;
            });
        }

        @Override
        public StatusUpdate<MergeProgress> toStatusUpdate() {
            long total = totalVectors.get();
            long merged = mergedVectors.get();
            RunState currentState = state.get();
            double progress = total > 0 ? Math.min(1.0, (double) merged / total) : 0.0;
            if (currentState == RunState.SUCCESS) {
                progress = 1.0;
            }
            return new StatusUpdate<>(progress, currentState, this);
        }

        @Override
        public String getName() {
            long total = totalVectors.get();
            long merged = mergedVectors.get();
            if (total > 0) {
                return String.format("Chunk Merge (%,d/%,d vectors)", merged, total);
            }
            return String.format("Chunk Merge (%,d vectors)", merged);
        }
    }

    private interface RunnableStatus<T extends RunnableStatus<T>> extends io.nosqlbench.status.eventing.StatusSource<T> {
        StatusUpdate<T> toStatusUpdate();

        @Override
        default StatusUpdate<T> getTaskStatus() {
            return toStatusUpdate();
        }

        String getName();
    }

    /**
     * Parse the range specification
     * Formats: 'n' (0 to n-1), 'm..n' (m to n inclusive), '[m,n)' (m to n-1)
     */
    private void parseRange() {
        if (range == null || range.trim().isEmpty()) {
            return;
        }

        rangeSpecified = true;
        String r = range.trim();

        try {
            // Format: [m,n) - closed-open interval
            if (r.startsWith("[") && r.endsWith(")")) {
                String inner = r.substring(1, r.length() - 1);
                String[] parts = inner.split(",");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid range format: " + range);
                }
                rangeStart = Long.parseLong(parts[0].trim());
                rangeEnd = Long.parseLong(parts[1].trim());
            }
            // Format: m..n - closed interval (inclusive)
            else if (r.contains("..")) {
                String[] parts = r.split("\\.\\.");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid range format: " + range);
                }
                rangeStart = Long.parseLong(parts[0].trim());
                rangeEnd = Long.parseLong(parts[1].trim()) + 1; // Make exclusive
            }
            // Format: n - from 0 to n-1
            else {
                rangeStart = 0;
                rangeEnd = Long.parseLong(r);
            }

            // Validate range
            if (rangeStart < 0) {
                throw new IllegalArgumentException("Range start must be non-negative: " + rangeStart);
            }
            if (rangeEnd <= rangeStart) {
                throw new IllegalArgumentException("Range end must be greater than start: [" + rangeStart + ", " + rangeEnd + ")");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid range format: " + range + " - " + e.getMessage());
        }
    }

    /**
     * Validates the input and output paths before execution
     */
    private void validatePaths() {
        if (inputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No input path provided");
        }

        if (outputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No output path provided");
        }

        if (!Files.exists(inputPath)) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Input file does not exist: " + inputPath);
        }

        if (chunkSize <= 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Chunk size must be positive");
        }

        // Normalize paths
        inputPath = inputPath.normalize();
        outputPath = outputPath.normalize();
        if (tempDir != null) {
            tempDir = tempDir.normalize();
        }
    }

    /**
     * Resource configuration for parallel sorting
     */
    private static class ResourceConfig {
        final int threadCount;
        final int maxConcurrentChunks;
        final long estimatedChunkMemoryBytes;

        ResourceConfig(int threadCount, int maxConcurrentChunks, long estimatedChunkMemoryBytes) {
            this.threadCount = threadCount;
            this.maxConcurrentChunks = maxConcurrentChunks;
            this.estimatedChunkMemoryBytes = estimatedChunkMemoryBytes;
        }
    }

    /**
     * Calculate optimal resource configuration based on available memory and CPU cores.
     * Always leaves at least 1 hardware thread unallocated for system processes.
     *
     * @return ResourceConfig with optimal thread count and concurrent chunk limit
     */
    private ResourceConfig calculateResourceConfig() {
        // Determine thread count
        int availableCores = Runtime.getRuntime().availableProcessors();
        int optimalThreads;

        if (explicitThreads != null) {
            // User specified thread count
            optimalThreads = Math.max(1, explicitThreads);
            if (optimalThreads >= availableCores) {
                logger.warn("Specified thread count ({}) >= available cores ({}). This may cause contention.",
                    optimalThreads, availableCores);
            }
        } else if (parallel) {
            // Auto-detect: use all but 1 core (leaving 1 free for system)
            optimalThreads = Math.max(1, availableCores - 1);
        } else {
            // Sequential mode
            optimalThreads = 1;
        }

        // Estimate memory requirements
        Runtime runtime = Runtime.getRuntime();
        long maxHeapBytes = runtime.maxMemory();  // -Xmx value
        long totalMemory = runtime.totalMemory(); // Currently allocated
        long freeMemory = runtime.freeMemory();   // Free within allocated
        long availableMemory = maxHeapBytes - (totalMemory - freeMemory);

        // Reserve 20% of max heap for JVM overhead, merge operations, and other data structures
        long usableMemory = (long) (maxHeapBytes * 0.8);

        // Estimate bytes per vector based on chunk size
        // Assume average: float[128] = 512 bytes, double[128] = 1024 bytes
        // Use conservative estimate of 1KB per vector for safety
        long estimatedBytesPerVector = 1024;
        long estimatedChunkMemoryBytes = chunkSize * estimatedBytesPerVector;

        // Calculate max concurrent chunks that fit in memory
        int maxConcurrentChunks;
        if (parallel) {
            // Allow enough memory for concurrent chunks with safety margin
            maxConcurrentChunks = (int) (usableMemory / estimatedChunkMemoryBytes);
            // Cap at thread count (no point having more chunks than threads)
            maxConcurrentChunks = Math.min(maxConcurrentChunks, optimalThreads);
            // Ensure at least 1
            maxConcurrentChunks = Math.max(1, maxConcurrentChunks);

            logger.debug("Memory analysis: max={}MB, available={}MB, usable={}MB, chunk={}MB, maxConcurrent={}",
                maxHeapBytes / (1024*1024),
                availableMemory / (1024*1024),
                usableMemory / (1024*1024),
                estimatedChunkMemoryBytes / (1024*1024),
                maxConcurrentChunks);
        } else {
            maxConcurrentChunks = 1;
        }

        return new ResourceConfig(optimalThreads, maxConcurrentChunks, estimatedChunkMemoryBytes);
    }

    /**
     * Determine if progress should be shown
     */
    private boolean shouldShowProgress() {
        if (showProgress != null) {
            return showProgress;
        }
        // Auto-detect: show progress if outputting to a TTY
        return System.console() != null;
    }

    private StatusSink chooseStatusSink() {
        if (!shouldShowProgress()) {
            return null;
        }
        if (System.console() != null) {
            try {
                return ConsolePanelSink.builder().build();
            } catch (Exception e) {
                logger.warn("ConsolePanelSink unavailable, falling back to ConsoleLoggerSink: {}", e.getMessage());
                return new ConsoleLoggerSink();
            }
        }
        return new ConsoleLoggerSink();
    }

    @Override
    public Integer call() throws Exception {
        try {
            validatePaths();
            parseRange();
        } catch (CommandLine.ParameterException | IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return EXIT_ERROR;
        }

        // Check if output file exists
        if (Files.exists(outputPath) && !force) {
            System.err.println("Error: Output file already exists. Use --force to overwrite.");
            return EXIT_FILE_EXISTS;
        }

        ChunkSortProgress chunkProgress = new ChunkSortProgress();
        MergeProgress mergeProgress = new MergeProgress();
        StatusSink sink = chooseStatusSink();

        try {
            // Create parent directories if needed
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Set up temp directory
            Path actualTempDir = tempDir != null ? tempDir : Files.createTempDirectory("nbdatatools-sort");
            if (tempDir != null) {
                Files.createDirectories(actualTempDir);
            }

            // Calculate optimal resource configuration
            ResourceConfig resourceConfig = calculateResourceConfig();

            logger.info("Starting external merge sort...");
            logger.info("Input: {}", inputPath);
            logger.info("Output: {}", outputPath);
            if (rangeSpecified) {
                logger.info("Range: [{}, {}) - sorting {} vectors", rangeStart, rangeEnd, (rangeEnd - rangeStart));
            }
            logger.info("Chunk size: {} vectors", chunkSize);
            logger.info("Temp directory: {}", actualTempDir);
            if (resume) {
                logger.info("Resume mode: enabled (skipping already-completed chunks)");
            }
            logger.info("Concurrency: {} threads, max {} chunks in flight",
                resourceConfig.threadCount, resourceConfig.maxConcurrentChunks);

            String scopeName = inputPath.getFileName() != null
                ? inputPath.getFileName().toString()
                : "compute-sort-run";

            try (StatusContext context = new StatusContext("compute-sort");
                 StatusScope runScope = context.createScope(scopeName)) {

                if (sink != null) {
                    context.addSink(sink);
                }

                try (StatusTracker<ChunkSortProgress> ignoredChunkTracker = runScope.trackTask(chunkProgress);
                     StatusTracker<MergeProgress> ignoredMergeTracker = runScope.trackTask(mergeProgress)) {

                    // Detect file type and data type
                    FileType fileType = FileType.xvec;

                    // Determine data type from file extension
                    String fileName = inputPath.getFileName().toString().toLowerCase();
                    if (fileName.endsWith(".fvec") || fileName.endsWith(".fvecs")) {
                        sortFile(fileType, float[].class, actualTempDir, resourceConfig, chunkProgress, mergeProgress);
                    } else if (fileName.endsWith(".dvec") || fileName.endsWith(".dvecs")) {
                        sortFile(fileType, double[].class, actualTempDir, resourceConfig, chunkProgress, mergeProgress);
                    } else if (fileName.endsWith(".ivec") || fileName.endsWith(".ivecs")) {
                        sortFile(fileType, int[].class, actualTempDir, resourceConfig, chunkProgress, mergeProgress);
                    } else if (fileName.endsWith(".bvec") || fileName.endsWith(".bvecs")) {
                        sortFile(fileType, byte[].class, actualTempDir, resourceConfig, chunkProgress, mergeProgress);
                    } else {
                        System.err.println("Error: Unsupported file type. Supported: .fvec, .dvec, .ivec, .bvec");
                        chunkProgress.fail();
                        mergeProgress.fail();
                        return EXIT_ERROR;
                    }
                }
            } finally {
                if (sink instanceof AutoCloseable) {
                    AutoCloseable closable = (AutoCloseable) sink;
                    try {
                        closable.close();
                    } catch (Exception e) {
                        logger.debug("Error closing status sink: {}", e.getMessage(), e);
                    }
                }
            }

            // Clean up temp directory if we created it
            if (tempDir == null) {
                deleteDirectory(actualTempDir);
            }

            logger.info("Sort completed successfully!");
            return EXIT_SUCCESS;

        } catch (IOException e) {
            chunkProgress.fail();
            mergeProgress.fail();
            System.err.println("Error: I/O problem - " + e.getMessage());
            logger.error("I/O error during sort", e);
            return EXIT_ERROR;
        } catch (Exception e) {
            chunkProgress.fail();
            mergeProgress.fail();
            System.err.println("Error sorting vectors: " + e.getMessage());
            logger.error("Error during sort", e);
            return EXIT_ERROR;
        }
    }

    /**
     * Sort a file with a specific data type using external merge sort
     */
    private <T> void sortFile(FileType fileType,
                              Class<T> dataClass,
                              Path tempDir,
                              ResourceConfig resourceConfig,
                              ChunkSortProgress chunkProgress,
                              MergeProgress mergeProgress) throws IOException {
        // Phase 1: Read input, sort chunks, write to temp files
        List<Path> chunkFiles = sortAndWriteChunks(fileType, dataClass, tempDir, resourceConfig, chunkProgress);

        if (chunkFiles.isEmpty()) {
            System.err.println("Error: No data to sort");
            throw new IllegalStateException("No data in input file");
        }

        logger.info("Created {} sorted chunk(s)", chunkFiles.size());

        // Phase 2: Merge sorted chunks into output file
        if (chunkFiles.size() == 1) {
            // Only one chunk - just rename/copy it
            Files.move(chunkFiles.get(0), outputPath);
            logger.info("Single chunk - moved directly to output");
            mergeProgress.start(1);
            mergeProgress.updateMerged(1);
            mergeProgress.success();
        } else {
            // Multiple chunks - perform k-way merge
            mergeChunks(fileType, dataClass, chunkFiles, mergeProgress);
            logger.info("Merged {} chunks into output file", chunkFiles.size());

            // Clean up chunk files
            for (Path chunkFile : chunkFiles) {
                Files.deleteIfExists(chunkFile);
            }

            mergeProgress.success();
        }
    }

    /**
     * Result of sorting a chunk
     */
    private static class ChunkResult {
        final Path chunkFile;
        final int chunkNumber;
        final int vectorCount;
        final Exception error;

        ChunkResult(Path chunkFile, int chunkNumber, int vectorCount, Exception error) {
            this.chunkFile = chunkFile;
            this.chunkNumber = chunkNumber;
            this.vectorCount = vectorCount;
            this.error = error;
        }
    }

    /**
     * Phase 1: Read vectors in chunks, sort each chunk, write to temp files
     * Uses thread pool with configured concurrency (1 thread = sequential, N threads = parallel)
     * Each worker thread uses its own random access reader to read its assigned chunk directly
     */
    private <T> List<Path> sortAndWriteChunks(FileType fileType,
                                              Class<T> dataClass,
                                              Path tempDir,
                                              ResourceConfig resourceConfig,
                                              ChunkSortProgress chunkProgress) throws IOException {
        List<Path> chunkFiles = Collections.synchronizedList(new ArrayList<>());
        List<Future<ChunkResult>> futures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(
            resourceConfig.threadCount,
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "sort-worker-" + counter.incrementAndGet());
                    t.setDaemon(false);
                    return t;
                }
            }
        );

        Semaphore memoryLimiter = new Semaphore(resourceConfig.maxConcurrentChunks);
        AtomicInteger chunkCounter = new AtomicInteger(0);
        AtomicInteger skippedChunks = new AtomicInteger(0);

        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, inputPath);
        int totalVectorsInFile = reader.size();

        long effectiveStart = rangeSpecified ? rangeStart : 0;
        long effectiveEnd = rangeSpecified ? Math.min(rangeEnd, totalVectorsInFile) : totalVectorsInFile;

        if (effectiveStart >= totalVectorsInFile) {
            throw new IOException("Range start " + effectiveStart + " is beyond file size " + totalVectorsInFile);
        }

        int vectorsToSort = (int) (effectiveEnd - effectiveStart);
        int expectedChunks = (vectorsToSort + chunkSize - 1) / chunkSize;

        chunkProgress.start(expectedChunks, vectorsToSort);

        if (vectorsToSort > 0) {
            initializeVectorDimensions(reader.get((int) effectiveStart), dataClass);
        }

        if (resume) {
            for (int chunkNum = 0; chunkNum < expectedChunks; chunkNum++) {
                int expectedVectorCount = (chunkNum == expectedChunks - 1)
                    ? (vectorsToSort - chunkNum * chunkSize)
                    : chunkSize;

                if (isChunkValid(chunkNum, expectedVectorCount, fileType, dataClass, tempDir)) {
                    String extension = getExtension(dataClass);
                    Path chunkFile = tempDir.resolve("chunk_" + chunkNum + extension);

                    chunkProgress.recordChunk(expectedVectorCount, true);
                    skippedChunks.incrementAndGet();
                    chunkCounter.incrementAndGet();
                    chunkFiles.add(chunkFile);
                } else {
                    break;
                }
            }
        }

        if (chunkCounter.get() >= expectedChunks) {
            logger.info("All chunks already exist - skipping sort phase");
            executor.shutdown();
            chunkProgress.success();
            try {
                reader.close();
            } catch (Exception e) {
                logger.debug("Error closing reader after resume: {}", e.getMessage(), e);
            }
            return chunkFiles;
        }

        for (int chunkNum = chunkCounter.get(); chunkNum < expectedChunks; chunkNum++) {
            final int finalChunkNum = chunkNum;
            final int startIdxRelative = chunkNum * chunkSize;
            final int endIdxRelative = Math.min(startIdxRelative + chunkSize, vectorsToSort);
            final int startIdx = (int) effectiveStart + startIdxRelative;
            final int endIdx = (int) effectiveStart + endIdxRelative;
            final int chunkVectorCount = endIdx - startIdx;

            try {
                memoryLimiter.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for memory", e);
            }

            Future<ChunkResult> future = executor.submit(() -> {
                try (VectorFileArray<T> workerReader = VectorFileIO.randomAccess(fileType, dataClass, inputPath)) {
                    List<T> chunkToSort = new ArrayList<>(chunkVectorCount);
                    for (int i = startIdx; i < endIdx; i++) {
                        chunkToSort.add(workerReader.get(i));
                    }

                    Path chunkFile = writeChunk(chunkToSort, finalChunkNum, fileType, dataClass, tempDir);
                    chunkProgress.recordChunk(chunkVectorCount, false);
                    return new ChunkResult(chunkFile, finalChunkNum, chunkVectorCount, null);
                } catch (Exception e) {
                    return new ChunkResult(null, finalChunkNum, chunkVectorCount, e);
                } finally {
                    memoryLimiter.release();
                }
            });

            futures.add(future);
        }

        try {
            for (Future<ChunkResult> future : futures) {
                ChunkResult result = future.get();
                if (result.error != null) {
                    chunkProgress.fail();
                    throw new IOException("Error sorting chunk " + result.chunkNumber, result.error);
                }
                chunkFiles.add(result.chunkFile);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            chunkProgress.fail();
            throw new IOException("Interrupted while waiting for chunk completion", e);
        } catch (ExecutionException e) {
            chunkProgress.fail();
            throw new IOException("Error during parallel chunk sorting", e.getCause());
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                logger.debug("Error closing reader: {}", e.getMessage(), e);
            }
        }

        int skipped = skippedChunks.get();
        if (skipped > 0) {
            logger.info("Total vectors processed: {} ({} chunks resumed)", vectorsToSort, skipped);
        } else {
            logger.info("Total vectors processed: {}", vectorsToSort);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        chunkFiles.sort(Comparator.comparing(path -> {
            String name = path.getFileName().toString();
            int underscoreIdx = name.indexOf('_');
            int dotIdx = name.indexOf('.');
            if (underscoreIdx >= 0 && dotIdx > underscoreIdx) {
                return Integer.parseInt(name.substring(underscoreIdx + 1, dotIdx));
            }
            return 0;
        }));

        chunkProgress.success();
        return chunkFiles;
    }

    // Cache the vector dimensions and bytes per element after first vector is read
    private volatile Integer cachedVectorDimensions = null;
    private volatile Integer cachedBytesPerElement = null;

    /**
     * Check if a chunk file exists and has the expected size
     * @return true if the chunk file is valid and complete
     */
    private <T> boolean isChunkValid(int chunkNumber, int expectedVectorCount, FileType fileType, Class<T> dataClass, Path tempDir) {
        String extension = getExtension(dataClass);
        Path chunkFile = tempDir.resolve("chunk_" + chunkNumber + extension);

        if (!Files.exists(chunkFile)) {
            return false;
        }

        // If we have cached dimensions, verify the file size
        if (cachedVectorDimensions != null && cachedBytesPerElement != null) {
            try {
                long actualSize = Files.size(chunkFile);
                // xvec format: 4 bytes (dimension) + dimension * bytesPerElement per vector
                long expectedSize = expectedVectorCount * (4L + cachedVectorDimensions * cachedBytesPerElement);
                return actualSize == expectedSize;
            } catch (IOException e) {
                // If we can't read file size, assume it's invalid
                return false;
            }
        }

        // Fast path: just check if the file exists
        // We trust that if the file exists, it was created by a previous run and is valid
        return true;
    }

    /**
     * Initialize the cached vector dimensions from the first vector
     */
    private <T> void initializeVectorDimensions(T vector, Class<T> dataClass) {
        if (cachedVectorDimensions == null) {
            if (dataClass == double[].class) {
                cachedVectorDimensions = ((double[]) vector).length;
                cachedBytesPerElement = 8;
            } else if (dataClass == float[].class) {
                cachedVectorDimensions = ((float[]) vector).length;
                cachedBytesPerElement = 4;
            } else if (dataClass == int[].class) {
                cachedVectorDimensions = ((int[]) vector).length;
                cachedBytesPerElement = 4;
            } else if (dataClass == byte[].class) {
                cachedVectorDimensions = ((byte[]) vector).length;
                cachedBytesPerElement = 1;
            }
        }
    }

    /**
     * Sort a chunk in memory and write it to a temp file
     */
    private <T> Path writeChunk(List<T> chunk, int chunkNumber, FileType fileType, Class<T> dataClass, Path tempDir) throws IOException {
        // Sort the chunk using appropriate algorithm based on type
        if (dataClass == double[].class) {
            @SuppressWarnings("unchecked")
            List<double[]> doubleChunk = (List<double[]>) chunk;
            AmericanFlagRadixSort.sort(doubleChunk);
        } else if (dataClass == float[].class) {
            @SuppressWarnings("unchecked")
            List<float[]> floatChunk = (List<float[]>) chunk;
            AmericanFlagRadixSort.sortFloat(floatChunk);
        } else if (dataClass == int[].class) {
            @SuppressWarnings("unchecked")
            List<int[]> intChunk = (List<int[]>) chunk;
            AmericanFlagRadixSort.sortInt(intChunk);
        } else if (dataClass == byte[].class) {
            @SuppressWarnings("unchecked")
            List<byte[]> byteChunk = (List<byte[]>) chunk;
            AmericanFlagRadixSort.sortByte(byteChunk);
        }

        // Write sorted chunk to temp file
        String extension = getExtension(dataClass);
        Path chunkFile = tempDir.resolve("chunk_" + chunkNumber + extension);

        try (VectorFileStreamStore<T> output = VectorFileIO.streamOut(fileType, dataClass, chunkFile)
            .orElseThrow(() -> new RuntimeException("Could not create chunk file: " + chunkFile))) {

            for (T vector : chunk) {
                output.write(vector);
            }
        }

        return chunkFile;
    }

    /**
     * Phase 2: Merge sorted chunks using k-way merge
     */
    private <T> void mergeChunks(FileType fileType,
                                 Class<T> dataClass,
                                 List<Path> chunkFiles,
                                 MergeProgress mergeProgress) throws IOException {
        // Open all chunk files
        List<BoundedVectorFileStream<T>> streams = new ArrayList<>();
        List<Iterator<T>> iterators = new ArrayList<>();

        for (Path chunkFile : chunkFiles) {
            BoundedVectorFileStream<T> stream = VectorFileIO.streamIn(fileType, dataClass, chunkFile)
                .orElseThrow(() -> new RuntimeException("Could not open chunk file: " + chunkFile));
            streams.add(stream);
            iterators.add(stream.iterator());
        }

        try (VectorFileStreamStore<T> output = VectorFileIO.streamOut(fileType, dataClass, outputPath)
            .orElseThrow(() -> new RuntimeException("Could not create output file: " + outputPath))) {

            // Use a priority queue for k-way merge
            Comparator<VectorEntry<T>> comparator = createComparator(dataClass);
            PriorityQueue<VectorEntry<T>> minHeap = new PriorityQueue<>(comparator);

            // Initialize heap with first vector from each chunk
            for (int i = 0; i < iterators.size(); i++) {
                Iterator<T> it = iterators.get(i);
                if (it.hasNext()) {
                    minHeap.offer(new VectorEntry<>(it.next(), i));
                }
            }

            // Calculate total vectors to merge for progress tracking
            long totalVectorsToMerge = streams.stream().mapToLong(s -> s.getSize()).sum();
            mergeProgress.start(totalVectorsToMerge);

            int mergedCount = 0;

            // Extract minimum and refill from same chunk
            while (!minHeap.isEmpty()) {
                VectorEntry<T> entry = minHeap.poll();
                output.write(entry.vector);
                mergedCount++;
                mergeProgress.updateMerged(mergedCount);

                // Refill from the same chunk
                Iterator<T> it = iterators.get(entry.chunkIndex);
                if (it.hasNext()) {
                    minHeap.offer(new VectorEntry<>(it.next(), entry.chunkIndex));
                }
            }

            mergeProgress.updateMerged(mergedCount);
            logger.info("Total merged vectors: {}", mergedCount);
        } catch (Exception e) {
            mergeProgress.fail();
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Error merging chunks", e);
        } finally {
            // Close all streams
            for (BoundedVectorFileStream<T> stream : streams) {
                try {
                    stream.close();
                } catch (Exception e) {
                    logger.warn("Error closing stream", e);
                }
            }
        }
    }

    /**
     * Create a comparator for the specific data type
     */
    private <T> Comparator<VectorEntry<T>> createComparator(Class<T> dataClass) {
        if (dataClass == double[].class) {
            return (a, b) -> AmericanFlagRadixSort.compareDouble((double[]) a.vector, (double[]) b.vector);
        } else if (dataClass == float[].class) {
            return (a, b) -> AmericanFlagRadixSort.compareFloat((float[]) a.vector, (float[]) b.vector);
        } else if (dataClass == int[].class) {
            return (a, b) -> AmericanFlagRadixSort.compareInt((int[]) a.vector, (int[]) b.vector);
        } else if (dataClass == byte[].class) {
            return (a, b) -> AmericanFlagRadixSort.compareByte((byte[]) a.vector, (byte[]) b.vector);
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataClass);
    }

    /**
     * Get file extension for data type
     */
    private String getExtension(Class<?> dataClass) {
        if (dataClass == double[].class) return ".dvec";
        if (dataClass == float[].class) return ".fvec";
        if (dataClass == int[].class) return ".ivec";
        if (dataClass == byte[].class) return ".bvec";
        throw new IllegalArgumentException("Unsupported data type: " + dataClass);
    }

    /**
     * Helper class for k-way merge
     */
    private static class VectorEntry<T> {
        final T vector;
        final int chunkIndex;

        VectorEntry(T vector, int chunkIndex) {
            this.vector = vector;
            this.chunkIndex = chunkIndex;
        }
    }

    /**
     * Recursively delete a directory
     */
    private void deleteDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        logger.warn("Could not delete: " + path, e);
                    }
                });
        }
    }

    /**
     * American Flag Radix Sort implementation for vectors
     *
     * This is a byte-level MSD (Most Significant Digit) radix sort that processes
     * vectors lexicographically. It's cache-friendly and works well for high-dimensional
     * vectors.
     */
    private static class AmericanFlagRadixSort {
        private static final int INSERTION_SORT_THRESHOLD = 32;
        private static final int BYTES_PER_DOUBLE = 8;
        private static final int BYTES_PER_FLOAT = 4;
        private static final int BYTES_PER_INT = 4;

        /**
         * Sort double[][] vectors lexicographically
         */
        public static void sort(List<double[]> vectors) {
            if (vectors == null || vectors.size() <= 1) return;

            // Convert to array for efficient swapping
            double[][] arr = vectors.toArray(new double[0][]);

            // Validate dimension consistency
            final int M = arr[0].length;
            for (double[] v : arr) {
                if (v.length != M) {
                    throw new IllegalArgumentException("Ragged vectors not supported");
                }
            }

            final int TOTAL_DIGITS = M * BYTES_PER_DOUBLE;
            sortRangeDouble(arr, 0, arr.length, 0, TOTAL_DIGITS);

            // Copy back to list
            for (int i = 0; i < arr.length; i++) {
                vectors.set(i, arr[i]);
            }
        }

        /**
         * Sort float[][] vectors lexicographically
         */
        public static void sortFloat(List<float[]> vectors) {
            if (vectors == null || vectors.size() <= 1) return;

            float[][] arr = vectors.toArray(new float[0][]);
            final int M = arr[0].length;
            for (float[] v : arr) {
                if (v.length != M) {
                    throw new IllegalArgumentException("Ragged vectors not supported");
                }
            }

            final int TOTAL_DIGITS = M * BYTES_PER_FLOAT;
            sortRangeFloat(arr, 0, arr.length, 0, TOTAL_DIGITS);

            for (int i = 0; i < arr.length; i++) {
                vectors.set(i, arr[i]);
            }
        }

        /**
         * Sort int[][] vectors lexicographically
         */
        public static void sortInt(List<int[]> vectors) {
            if (vectors == null || vectors.size() <= 1) return;

            int[][] arr = vectors.toArray(new int[0][]);
            final int M = arr[0].length;
            for (int[] v : arr) {
                if (v.length != M) {
                    throw new IllegalArgumentException("Ragged vectors not supported");
                }
            }

            final int TOTAL_DIGITS = M * BYTES_PER_INT;
            sortRangeInt(arr, 0, arr.length, 0, TOTAL_DIGITS);

            for (int i = 0; i < arr.length; i++) {
                vectors.set(i, arr[i]);
            }
        }

        /**
         * Sort byte[][] vectors lexicographically
         */
        public static void sortByte(List<byte[]> vectors) {
            if (vectors == null || vectors.size() <= 1) return;

            byte[][] arr = vectors.toArray(new byte[0][]);
            final int M = arr[0].length;
            for (byte[] v : arr) {
                if (v.length != M) {
                    throw new IllegalArgumentException("Ragged vectors not supported");
                }
            }

            // For bytes, each element is already a byte
            sortRangeByte(arr, 0, arr.length, 0, M);

            for (int i = 0; i < arr.length; i++) {
                vectors.set(i, arr[i]);
            }
        }

        // --- Core sorting routines ---

        private static void sortRangeDouble(double[][] A, int from, int to, int digit, int TOTAL_DIGITS) {
            final int n = to - from;
            if (n <= INSERTION_SORT_THRESHOLD || digit >= TOTAL_DIGITS) {
                insertionSortDouble(A, from, to);
                return;
            }

            int[] cnt = new int[256];
            for (int i = from; i < to; i++) {
                cnt[byteOfDouble(A[i], digit)]++;
            }

            int[] start = new int[256];
            int sum = from;
            for (int b = 0; b < 256; b++) {
                start[b] = sum;
                sum += cnt[b];
            }

            int[] next = Arrays.copyOf(start, 256);
            int[] end = new int[256];
            for (int b = 0; b < 256; b++) {
                end[b] = start[b] + cnt[b];
            }

            // Cycle-leader permutation
            for (int i = from; i < to; ) {
                int b = byteOfDouble(A[i], digit);
                if (i >= start[b] && i < next[b]) {
                    i++;
                } else if (next[b] >= end[b]) {
                    i++;
                } else {
                    swapDouble(A, i, next[b]);
                    next[b]++;
                }
            }

            // Recurse
            if (digit + 1 < TOTAL_DIGITS) {
                for (int b = 0; b < 256; b++) {
                    if (end[b] - start[b] > 1) {
                        sortRangeDouble(A, start[b], end[b], digit + 1, TOTAL_DIGITS);
                    }
                }
            }
        }

        private static void sortRangeFloat(float[][] A, int from, int to, int digit, int TOTAL_DIGITS) {
            final int n = to - from;
            if (n <= INSERTION_SORT_THRESHOLD || digit >= TOTAL_DIGITS) {
                insertionSortFloat(A, from, to);
                return;
            }

            int[] cnt = new int[256];
            for (int i = from; i < to; i++) {
                cnt[byteOfFloat(A[i], digit)]++;
            }

            int[] start = new int[256];
            int sum = from;
            for (int b = 0; b < 256; b++) {
                start[b] = sum;
                sum += cnt[b];
            }

            int[] next = Arrays.copyOf(start, 256);
            int[] end = new int[256];
            for (int b = 0; b < 256; b++) {
                end[b] = start[b] + cnt[b];
            }

            for (int i = from; i < to; ) {
                int b = byteOfFloat(A[i], digit);
                if (i >= start[b] && i < next[b]) {
                    i++;
                } else if (next[b] >= end[b]) {
                    i++;
                } else {
                    swapFloat(A, i, next[b]);
                    next[b]++;
                }
            }

            if (digit + 1 < TOTAL_DIGITS) {
                for (int b = 0; b < 256; b++) {
                    if (end[b] - start[b] > 1) {
                        sortRangeFloat(A, start[b], end[b], digit + 1, TOTAL_DIGITS);
                    }
                }
            }
        }

        private static void sortRangeInt(int[][] A, int from, int to, int digit, int TOTAL_DIGITS) {
            final int n = to - from;
            if (n <= INSERTION_SORT_THRESHOLD || digit >= TOTAL_DIGITS) {
                insertionSortInt(A, from, to);
                return;
            }

            int[] cnt = new int[256];
            for (int i = from; i < to; i++) {
                cnt[byteOfInt(A[i], digit)]++;
            }

            int[] start = new int[256];
            int sum = from;
            for (int b = 0; b < 256; b++) {
                start[b] = sum;
                sum += cnt[b];
            }

            int[] next = Arrays.copyOf(start, 256);
            int[] end = new int[256];
            for (int b = 0; b < 256; b++) {
                end[b] = start[b] + cnt[b];
            }

            for (int i = from; i < to; ) {
                int b = byteOfInt(A[i], digit);
                if (i >= start[b] && i < next[b]) {
                    i++;
                } else if (next[b] >= end[b]) {
                    i++;
                } else {
                    swapInt(A, i, next[b]);
                    next[b]++;
                }
            }

            if (digit + 1 < TOTAL_DIGITS) {
                for (int b = 0; b < 256; b++) {
                    if (end[b] - start[b] > 1) {
                        sortRangeInt(A, start[b], end[b], digit + 1, TOTAL_DIGITS);
                    }
                }
            }
        }

        private static void sortRangeByte(byte[][] A, int from, int to, int digit, int TOTAL_DIGITS) {
            final int n = to - from;
            if (n <= INSERTION_SORT_THRESHOLD || digit >= TOTAL_DIGITS) {
                insertionSortByte(A, from, to);
                return;
            }

            int[] cnt = new int[256];
            for (int i = from; i < to; i++) {
                cnt[byteOfByte(A[i], digit)]++;
            }

            int[] start = new int[256];
            int sum = from;
            for (int b = 0; b < 256; b++) {
                start[b] = sum;
                sum += cnt[b];
            }

            int[] next = Arrays.copyOf(start, 256);
            int[] end = new int[256];
            for (int b = 0; b < 256; b++) {
                end[b] = start[b] + cnt[b];
            }

            for (int i = from; i < to; ) {
                int b = byteOfByte(A[i], digit);
                if (i >= start[b] && i < next[b]) {
                    i++;
                } else if (next[b] >= end[b]) {
                    i++;
                } else {
                    swapByte(A, i, next[b]);
                    next[b]++;
                }
            }

            if (digit + 1 < TOTAL_DIGITS) {
                for (int b = 0; b < 256; b++) {
                    if (end[b] - start[b] > 1) {
                        sortRangeByte(A, start[b], end[b], digit + 1, TOTAL_DIGITS);
                    }
                }
            }
        }

        // --- Byte extraction ---

        private static int byteOfDouble(double[] vec, int digit) {
            int dim = digit / BYTES_PER_DOUBLE;
            int off = digit % BYTES_PER_DOUBLE;
            long key = transformDouble(vec[dim]);
            int shift = (7 - off) * 8;
            return (int) ((key >>> shift) & 0xFFL);
        }

        private static int byteOfFloat(float[] vec, int digit) {
            int dim = digit / BYTES_PER_FLOAT;
            int off = digit % BYTES_PER_FLOAT;
            int key = transformFloat(vec[dim]);
            int shift = (3 - off) * 8;
            return (key >>> shift) & 0xFF;
        }

        private static int byteOfInt(int[] vec, int digit) {
            int dim = digit / BYTES_PER_INT;
            int off = digit % BYTES_PER_INT;
            int key = transformInt(vec[dim]);
            int shift = (3 - off) * 8;
            return (key >>> shift) & 0xFF;
        }

        private static int byteOfByte(byte[] vec, int digit) {
            // For byte arrays, digit maps directly to index
            // Transform to unsigned 0-255 range
            return vec[digit] & 0xFF;
        }

        // --- Transform to lexicographic order ---

        private static long transformDouble(double x) {
            if (Double.isNaN(x)) return 0xFFFFFFFFFFFFFFFFL;
            long u = Double.doubleToRawLongBits(x);
            long sign = u >>> 63;
            if (sign == 1L) {
                return ~u;
            } else {
                return u ^ (1L << 63);
            }
        }

        private static int transformFloat(float x) {
            if (Float.isNaN(x)) return 0xFFFFFFFF;
            int u = Float.floatToRawIntBits(x);
            int sign = u >>> 31;
            if (sign == 1) {
                return ~u;
            } else {
                return u ^ (1 << 31);
            }
        }

        private static int transformInt(int x) {
            // Flip sign bit to make negatives < positives in unsigned order
            return x ^ (1 << 31);
        }

        // --- Insertion sort for small arrays ---

        private static void insertionSortDouble(double[][] A, int from, int to) {
            for (int i = from + 1; i < to; i++) {
                double[] key = A[i];
                int j = i - 1;
                while (j >= from && compareDouble(A[j], key) > 0) {
                    A[j + 1] = A[j];
                    j--;
                }
                A[j + 1] = key;
            }
        }

        private static void insertionSortFloat(float[][] A, int from, int to) {
            for (int i = from + 1; i < to; i++) {
                float[] key = A[i];
                int j = i - 1;
                while (j >= from && compareFloat(A[j], key) > 0) {
                    A[j + 1] = A[j];
                    j--;
                }
                A[j + 1] = key;
            }
        }

        private static void insertionSortInt(int[][] A, int from, int to) {
            for (int i = from + 1; i < to; i++) {
                int[] key = A[i];
                int j = i - 1;
                while (j >= from && compareInt(A[j], key) > 0) {
                    A[j + 1] = A[j];
                    j--;
                }
                A[j + 1] = key;
            }
        }

        private static void insertionSortByte(byte[][] A, int from, int to) {
            for (int i = from + 1; i < to; i++) {
                byte[] key = A[i];
                int j = i - 1;
                while (j >= from && compareByte(A[j], key) > 0) {
                    A[j + 1] = A[j];
                    j--;
                }
                A[j + 1] = key;
            }
        }

        // --- Lexicographic comparison ---

        public static int compareDouble(double[] a, double[] b) {
            int m = a.length;
            for (int d = 0; d < m; d++) {
                double x = a[d], y = b[d];
                boolean xNaN = Double.isNaN(x), yNaN = Double.isNaN(y);
                if (xNaN || yNaN) {
                    if (xNaN && !yNaN) return 1;   // NaN > number
                    if (!xNaN && yNaN) return -1;
                    continue; // both NaN
                }
                int c = Double.compare(x, y);
                if (c != 0) return c;
            }
            return 0;
        }

        public static int compareFloat(float[] a, float[] b) {
            int m = a.length;
            for (int d = 0; d < m; d++) {
                float x = a[d], y = b[d];
                boolean xNaN = Float.isNaN(x), yNaN = Float.isNaN(y);
                if (xNaN || yNaN) {
                    if (xNaN && !yNaN) return 1;
                    if (!xNaN && yNaN) return -1;
                    continue;
                }
                int c = Float.compare(x, y);
                if (c != 0) return c;
            }
            return 0;
        }

        public static int compareInt(int[] a, int[] b) {
            int m = a.length;
            for (int d = 0; d < m; d++) {
                int c = Integer.compare(a[d], b[d]);
                if (c != 0) return c;
            }
            return 0;
        }

        public static int compareByte(byte[] a, byte[] b) {
            int m = a.length;
            for (int d = 0; d < m; d++) {
                // Compare as unsigned
                int c = Integer.compare(a[d] & 0xFF, b[d] & 0xFF);
                if (c != 0) return c;
            }
            return 0;
        }

        // --- Swap operations ---

        private static void swapDouble(double[][] A, int i, int j) {
            if (i == j) return;
            double[] tmp = A[i];
            A[i] = A[j];
            A[j] = tmp;
        }

        private static void swapFloat(float[][] A, int i, int j) {
            if (i == j) return;
            float[] tmp = A[i];
            A[i] = A[j];
            A[j] = tmp;
        }

        private static void swapInt(int[][] A, int i, int j) {
            if (i == j) return;
            int[] tmp = A[i];
            A[i] = A[j];
            A[j] = tmp;
        }

        private static void swapByte(byte[][] A, int i, int j) {
            if (i == j) return;
            byte[] tmp = A[i];
            A[i] = A[j];
            A[j] = tmp;
        }
    }

    public static void main(String[] args) {
        CMD_compute_sort cmd = new CMD_compute_sort();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }
}
