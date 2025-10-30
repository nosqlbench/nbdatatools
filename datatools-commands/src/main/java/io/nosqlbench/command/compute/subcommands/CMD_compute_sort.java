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

import io.nosqlbench.command.common.ConsoleProgressOption;
import io.nosqlbench.command.common.InputFileOption;
import io.nosqlbench.command.common.OutputFileOption;
import io.nosqlbench.command.common.ParallelExecutionOption;
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.command.common.VerbosityOption;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.status.StatusContext;
import io.nosqlbench.status.StatusScope;
import io.nosqlbench.status.eventing.RunState;
import io.nosqlbench.status.eventing.StatusSource;
import io.nosqlbench.status.eventing.StatusUpdate;
import io.nosqlbench.status.exec.TrackedExecutorService;
import io.nosqlbench.status.exec.TrackedExecutors;
import io.nosqlbench.status.exec.TrackingMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    @CommandLine.Mixin
    private InputFileOption inputFileOption = new InputFileOption();

    @CommandLine.Mixin
    private OutputFileOption outputFileOption = new OutputFileOption();

    @CommandLine.Option(names = {"-c", "--chunk-size"},
        description = "Number of vectors per chunk (default: ${DEFAULT-VALUE})",
        defaultValue = "" + DEFAULT_CHUNK_SIZE)
    private int chunkSize = DEFAULT_CHUNK_SIZE;

    @CommandLine.Option(names = {"-t", "--temp-dir"},
        description = "Directory for temporary chunk files (default: system temp)")
    private Path tempDir;

    @CommandLine.Mixin
    private ParallelExecutionOption parallelExecutionOption = new ParallelExecutionOption();

    @CommandLine.Mixin
    private ConsoleProgressOption consoleProgressOption = new ConsoleProgressOption();

    @CommandLine.Mixin
    private VerbosityOption verbosityOption = new VerbosityOption();

    @CommandLine.Option(names = {"--dry-run", "-n"},
        description = "Show what would be done without actually doing it")
    private boolean dryRun = false;

    @CommandLine.Option(names = {"--resume"},
        description = "Resume from existing chunk files (skip chunks that are already complete)")
    private boolean resume = false;

    @CommandLine.Mixin
    private RangeOption rangeOption = new RangeOption();

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /**
     * Validates the input and output paths before execution
     */
    private void validatePaths() {
        inputFileOption.validate();
        // Output validation will be done later with force flag check

        if (chunkSize <= 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Chunk size must be positive");
        }

        // Normalize tempDir if provided
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
        // Determine thread count using the mixin
        int optimalThreads = parallelExecutionOption.getOptimalThreadCount();

        // Warn if user specified more threads than available cores
        if (parallelExecutionOption.exceedsAvailableCores()) {
            int availableCores = Runtime.getRuntime().availableProcessors();
            logger.warn("Specified thread count ({}) >= available cores ({}). This may cause contention.",
                parallelExecutionOption.getExplicitThreads(), availableCores);
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
        if (parallelExecutionOption.isEffectivelyParallel()) {
            // Allow enough memory for concurrent chunks with safety margin
            maxConcurrentChunks = (int) (usableMemory / estimatedChunkMemoryBytes);
            // Cap at thread count (no point having more chunks than threads)
            maxConcurrentChunks = Math.min(maxConcurrentChunks, optimalThreads);
            // Ensure at least 1
            maxConcurrentChunks = Math.max(1, maxConcurrentChunks);

            if (verbosityOption.showVerbose()) {
                printVerbose(String.format("Memory analysis: max=%dMB, available=%dMB, usable=%dMB, chunk=%dMB, maxConcurrent=%d",
                    maxHeapBytes / (1024*1024),
                    availableMemory / (1024*1024),
                    usableMemory / (1024*1024),
                    estimatedChunkMemoryBytes / (1024*1024),
                    maxConcurrentChunks));
            }
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
     * Print a simple message (respects quiet flag, thread-safe)
     */
    private synchronized void printMessage(String message) {
        if (verbosityOption.showNormalOutput()) {
            logger.info(message);
        }
    }

    /**
     * Print verbose message (only when verbose is enabled)
     */
    private synchronized void printVerbose(String message) {
        if (verbosityOption.showVerbose()) {
            logger.debug(message);
        }
    }

    /**
     * Configure log4j2 to write to ConsolePanelSink's log buffer instead of console
     */
    private void configureLog4j2ForConsolePanelSink(StatusContext ctx) {
        // Find ConsolePanelSink in the registered sinks
        io.nosqlbench.status.sinks.ConsolePanelSink panelSink = null;
        for (io.nosqlbench.status.eventing.StatusSink sink : ctx.getSinks()) {
            if (sink instanceof io.nosqlbench.status.sinks.ConsolePanelSink) {
                panelSink = (io.nosqlbench.status.sinks.ConsolePanelSink) sink;
                break;
            }
        }

        if (panelSink == null) {
            // No ConsolePanelSink found, nothing to configure
            return;
        }

        // Create a custom appender that writes to ConsolePanelSink
        final io.nosqlbench.status.sinks.ConsolePanelSink finalPanelSink = panelSink;
        org.apache.logging.log4j.core.LoggerContext loggerContext =
            (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = loggerContext.getConfiguration();

        // Create a custom appender that forwards to ConsolePanelSink
        org.apache.logging.log4j.core.Appender appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
            "ConsolePanelSinkAppender",
            null,
            org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
            true,
            null) {

            @Override
            public void append(org.apache.logging.log4j.core.LogEvent event) {
                // Format the log message and send it to ConsolePanelSink
                String formattedMessage = getLayout().toSerializable(event).toString();
                finalPanelSink.addLogMessage(formattedMessage);
            }
        };

        appender.start();

        // Add the appender to the root logger
        config.getRootLogger().addAppender(appender, null, null);

        // Update the logger context to apply changes
        loggerContext.updateLoggers();
    }

    @Override
    public Integer call() throws Exception {
        // Apply status mode configuration FIRST, then immediately create StatusContext
        // to ensure stream capture is active before ANY logging occurs
        consoleProgressOption.applyProgressMode();

        // Create StatusContext immediately - this creates the sink and captures streams
        try (StatusContext ctx = new StatusContext("vector-sort", consoleProgressOption.getProgressModeOptional())) {

            // Hook log4j2 to write to ConsolePanelSink's log buffer
            configureLog4j2ForConsolePanelSink(ctx);

            // Now ALL logging from here forward will be captured by the panel
            // Handle range specifications: inline input path range vs --range option
            RangeOption.Range effectiveRange = null;
            try {
                // Input file option automatically parses inline range during picocli processing
                String inlineRangeSpec = inputFileOption.getInlineRangeSpec();

                // Handle range: inline vs --range option
                if (inlineRangeSpec != null && rangeOption.isRangeSpecified()) {
                    throw new IllegalArgumentException(
                        "Range specified both in input path and via --range option. Please specify range only once.");
                }

                if (inlineRangeSpec != null) {
                    // Parse the inline range spec
                    effectiveRange = new RangeOption.RangeConverter().convert(inlineRangeSpec);
                } else if (rangeOption.isRangeSpecified()) {
                    // Use the --range option
                    effectiveRange = rangeOption.getRange();
                }
                // else: effectiveRange remains null (process all vectors)

                validatePaths();
            } catch (CommandLine.ParameterException | IllegalArgumentException e) {
                logger.error(e.getMessage());
                return EXIT_ERROR;
            }

            // Check if output file exists
            if (outputFileOption.outputExistsWithoutForce()) {
                logger.error("Error: Output file already exists. Use --force to overwrite.");
                return EXIT_FILE_EXISTS;
            }

            // Dry-run mode: show what would be done and exit
            if (dryRun) {
                printMessage("DRY RUN MODE - no files will be modified");
                printMessage("Input: " + inputFileOption.getInputPath());
                printMessage("Output: " + outputFileOption.getOutputPath());
                if (effectiveRange != null) {
                    printMessage("Range: " + effectiveRange.toString() + " - would sort " + effectiveRange.size() + " vectors");
                }
                printMessage("Chunk size: " + chunkSize + " vectors");
                printMessage("Parallel: " + parallelExecutionOption.isParallel());
                if (parallelExecutionOption.getExplicitThreads() != null) {
                    printMessage("Threads: " + parallelExecutionOption.getExplicitThreads());
                }
                printMessage("Force overwrite: " + outputFileOption.isForce());
                printMessage("Resume: " + resume);
                printMessage("\nDry run complete - no actions taken");
                return EXIT_SUCCESS;
            }

            try {
                // Create parent directories if needed
                Path outputPath = outputFileOption.getNormalizedOutputPath();
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

                printMessage("Starting external merge sort...");
                printMessage("Input: " + inputFileOption.getInputPath());
                printMessage("Output: " + outputPath);
                if (effectiveRange != null) {
                    printMessage("Range: " + effectiveRange.toString() + " - sorting " + effectiveRange.size() + " vectors");
                }
                printMessage("Chunk size: " + chunkSize + " vectors");
                printMessage("Temp directory: " + actualTempDir);
                if (resume) {
                    printMessage("Resume mode: enabled (skipping already-completed chunks)");
                }
                printMessage("Concurrency: " + resourceConfig.threadCount + " threads, max " +
                    resourceConfig.maxConcurrentChunks + " chunks in flight");

                // Detect file type and data type
                FileType fileType = FileType.xvec;

                // Determine data type from file extension
                Path inputPath = inputFileOption.getNormalizedInputPath();
                String fileName = inputPath.getFileName().toString().toLowerCase();
                if (fileName.endsWith(".fvec") || fileName.endsWith(".fvecs")) {
                    sortFile(ctx, fileType, float[].class, actualTempDir, resourceConfig, effectiveRange);
                } else if (fileName.endsWith(".dvec") || fileName.endsWith(".dvecs")) {
                    sortFile(ctx, fileType, double[].class, actualTempDir, resourceConfig, effectiveRange);
                } else if (fileName.endsWith(".ivec") || fileName.endsWith(".ivecs")) {
                    sortFile(ctx, fileType, int[].class, actualTempDir, resourceConfig, effectiveRange);
                } else if (fileName.endsWith(".bvec") || fileName.endsWith(".bvecs")) {
                    sortFile(ctx, fileType, byte[].class, actualTempDir, resourceConfig, effectiveRange);
                } else {
                    logger.error("Error: Unsupported file type. Supported: .fvec, .dvec, .ivec, .bvec");
                    return EXIT_ERROR;
                }

                // Clean up temp directory if we created it
                if (tempDir == null) {
                    deleteDirectory(actualTempDir);
                }

                printMessage("Sort completed successfully!");
                return EXIT_SUCCESS;

            } catch (IOException e) {
                logger.error("I/O error during sort: " + e.getMessage(), e);
                return EXIT_ERROR;
            } catch (Exception e) {
                logger.error("Error sorting vectors: " + e.getMessage(), e);
                return EXIT_ERROR;
            }
        }
    }

    /**
     * Sort a file with a specific data type using external merge sort
     */
    private <T> void sortFile(StatusContext ctx, FileType fileType, Class<T> dataClass, Path tempDir, ResourceConfig resourceConfig, RangeOption.Range effectiveRange) throws IOException {
        try (StatusScope sortScope = ctx.createScope("SortOperation")) {
            // Phase 1: Read input, sort chunks, write to temp files
            List<Path> chunkFiles = sortAndWriteChunks(sortScope, fileType, dataClass, tempDir, resourceConfig, effectiveRange);

            if (chunkFiles.isEmpty()) {
                logger.error("Error: No data to sort");
                throw new IllegalStateException("No data in input file");
            }

            printMessage("Created " + chunkFiles.size() + " sorted chunk(s)");

            // Phase 2: Merge sorted chunks into output file
            Path outputPath = outputFileOption.getNormalizedOutputPath();
            if (chunkFiles.size() == 1) {
                // Only one chunk - just rename/copy it
                Files.move(chunkFiles.get(0), outputPath);
                printMessage("Single chunk - moved directly to output");
            } else {
                // Multiple chunks - perform k-way merge
                mergeChunks(sortScope, fileType, dataClass, chunkFiles);
                printMessage("Merged " + chunkFiles.size() + " chunks into output file");

                // Clean up chunk files
                for (Path chunkFile : chunkFiles) {
                    Files.deleteIfExists(chunkFile);
                }
            }
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
     * Merge operation that implements StatusSource for progress tracking.
     * This concrete task encapsulates the k-way merge algorithm and reports its own progress.
     */
    private class MergeOperation<T> implements StatusSource<MergeOperation<T>> {
        private final FileType fileType;
        private final Class<T> dataClass;
        private final List<Path> chunkFiles;
        private final AtomicLong merged = new AtomicLong(0);
        private volatile long total = 0;
        private volatile RunState state = RunState.PENDING;

        MergeOperation(FileType fileType, Class<T> dataClass, List<Path> chunkFiles) {
            this.fileType = fileType;
            this.dataClass = dataClass;
            this.chunkFiles = chunkFiles;
        }

        /**
         * Execute the k-way merge operation
         */
        void execute() throws IOException {
            state = RunState.RUNNING;

            // Open all chunk files
            List<BoundedVectorFileStream<T>> streams = new ArrayList<>();
            List<Iterator<T>> iterators = new ArrayList<>();

            for (Path chunkFile : chunkFiles) {
                BoundedVectorFileStream<T> stream = VectorFileIO.streamIn(fileType, dataClass, chunkFile)
                    .orElseThrow(() -> new RuntimeException("Could not open chunk file: " + chunkFile));
                streams.add(stream);
                iterators.add(stream.iterator());
            }

            Path outputPath = outputFileOption.getNormalizedOutputPath();
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
                this.total = streams.stream().mapToLong(s -> s.getSize()).sum();
                int progressUpdateInterval = Math.max(1000, (int)(total / 1000)); // Update every 0.1%

                // Extract minimum and refill from same chunk
                while (!minHeap.isEmpty()) {
                    VectorEntry<T> entry = minHeap.poll();
                    output.write(entry.vector);
                    long currentMerged = merged.incrementAndGet();

                    // Update progress periodically
                    if (currentMerged % progressUpdateInterval == 0) {
                        // Progress is automatically reported through getTaskStatus()
                    }

                    // Refill from the same chunk
                    Iterator<T> it = iterators.get(entry.chunkIndex);
                    if (it.hasNext()) {
                        minHeap.offer(new VectorEntry<>(it.next(), entry.chunkIndex));
                    }
                }

                state = RunState.SUCCESS;
                printMessage("Total merged vectors: " + merged.get());
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

        @Override
        public StatusUpdate<MergeOperation<T>> getTaskStatus() {
            double progress = total > 0 ? (double) merged.get() / total : 0.0;
            return new StatusUpdate<>(progress, state, this);
        }

        @Override
        public String toString() {
            return String.format("Merging chunks [%d/%d vectors]", merged.get(), total);
        }
    }

    /**
     * Phase 1: Read vectors in chunks, sort each chunk, write to temp files
     * Uses TrackedExecutorService for automatic progress tracking
     * Each worker thread uses its own random access reader to read its assigned chunk directly
     */
    private <T> List<Path> sortAndWriteChunks(StatusScope sortScope, FileType fileType, Class<T> dataClass, Path tempDir,
                                               ResourceConfig resourceConfig, RangeOption.Range effectiveRange) throws IOException {
        List<Path> chunkFiles = Collections.synchronizedList(new ArrayList<>());
        List<Future<ChunkResult>> futures = new ArrayList<>();

        // Create thread pool with optimal thread count
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

        // Semaphore to limit concurrent chunks in memory
        Semaphore memoryLimiter = new Semaphore(resourceConfig.maxConcurrentChunks);

        AtomicInteger chunkCounter = new AtomicInteger(0);
        AtomicInteger totalVectors = new AtomicInteger(0);

        // Open random access reader to get file size and initialize dimensions
        Path inputPath = inputFileOption.getNormalizedInputPath();
        VectorFileArray<T> reader = VectorFileIO.randomAccess(fileType, dataClass, inputPath);
        int totalVectorsInFile = reader.size();

        // Apply range constraints
        RangeOption.Range constrainedRange = effectiveRange != null
            ? effectiveRange.constrain(totalVectorsInFile)
            : new RangeOption.Range(0, totalVectorsInFile);
        long effectiveStart = constrainedRange.start();
        long effectiveEnd = constrainedRange.end();

        if (effectiveStart >= totalVectorsInFile) {
            throw new IOException("Range start " + effectiveStart + " is beyond file size " + totalVectorsInFile);
        }

        int vectorsToSort = (int)(effectiveEnd - effectiveStart);
        int expectedChunks = (vectorsToSort + chunkSize - 1) / chunkSize;

        // Initialize dimensions from first vector in range for chunk validation
        if (vectorsToSort > 0) {
            initializeVectorDimensions(reader.get((int)effectiveStart), dataClass);
        }

        // Wrap executor with status tracking (use AGGREGATE mode for high chunk count)
        TrackingMode trackingMode = expectedChunks > 10 ? TrackingMode.AGGREGATE : TrackingMode.INDIVIDUAL;
        try (TrackedExecutorService trackedExecutor = TrackedExecutors.wrap(executor, sortScope)
                .withMode(trackingMode)
                .withTaskGroupName("Chunk Sorting")
                .build()) {

        // Now scan for existing chunks without holding the input stream open
        if (resume) {
            for (int chunkNum = 0; chunkNum < expectedChunks; chunkNum++) {
                int expectedVectorCount = (chunkNum == expectedChunks - 1)
                    ? (vectorsToSort - chunkNum * chunkSize)
                    : chunkSize;

                if (isChunkValid(chunkNum, expectedVectorCount, fileType, dataClass, tempDir)) {
                    String extension = getExtension(dataClass);
                    Path chunkFile = tempDir.resolve("chunk_" + chunkNum + extension);

                    // Create a completed future for the existing chunk
                    CompletableFuture<ChunkResult> completedFuture = CompletableFuture.completedFuture(
                        new ChunkResult(chunkFile, chunkNum, expectedVectorCount, null));
                    futures.add(completedFuture);

                    chunkCounter.incrementAndGet();
                    totalVectors.addAndGet(expectedVectorCount);
                    chunkFiles.add(chunkFile);
                } else {
                    // Stop at first missing chunk - we'll process from here
                    break;
                }
            }
        }

            // If all chunks exist, we're done
            if (chunkCounter.get() >= expectedChunks) {
                printMessage("All chunks already exist - skipping sort phase");
                return chunkFiles;
            }

            // Submit work for remaining chunks using random access
            // Each worker opens its own random access reader and reads its chunk directly by index
            for (int chunkNum = chunkCounter.get(); chunkNum < expectedChunks; chunkNum++) {
            final int finalChunkNum = chunkNum;
            final int startIdxRelative = chunkNum * chunkSize;
            final int endIdxRelative = Math.min(startIdxRelative + chunkSize, vectorsToSort);
            final int startIdx = (int)effectiveStart + startIdxRelative;
            final int endIdx = (int)effectiveStart + endIdxRelative;
            final int chunkVectorCount = endIdx - startIdx;

            // Acquire permit before submitting (blocks if too many chunks in flight)
            try {
                memoryLimiter.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for memory", e);
            }

            // Submit chunk for parallel sorting using tracked executor
            Future<ChunkResult> future = trackedExecutor.submit(() -> {
                try {
                    // Each worker opens its own random access reader
                    VectorFileArray<T> workerReader = VectorFileIO.randomAccess(fileType, dataClass, inputPath);

                    // Read the chunk into memory
                    List<T> chunkToSort = new ArrayList<>(chunkVectorCount);
                    for (int i = startIdx; i < endIdx; i++) {
                        chunkToSort.add(workerReader.get(i));
                    }

                    // Sort and write the chunk
                    Path chunkFile = writeChunk(chunkToSort, finalChunkNum, fileType, dataClass, tempDir);

                    return new ChunkResult(chunkFile, finalChunkNum, chunkVectorCount, null);
                } catch (Exception e) {
                    return new ChunkResult(null, finalChunkNum, chunkVectorCount, e);
                } finally {
                    memoryLimiter.release();
                }
            });

            futures.add(future);
        }

            // Wait for all chunks to complete and collect results
            for (int i = 0; i < futures.size(); i++) {
                try {
                    ChunkResult result = futures.get(i).get();
                    if (result.error != null) {
                        throw new IOException("Error sorting chunk " + result.chunkNumber, result.error);
                    }
                    if (!chunkFiles.contains(result.chunkFile)) {
                        chunkFiles.add(result.chunkFile);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for chunk completion", e);
                } catch (ExecutionException e) {
                    throw new IOException("Error during parallel chunk sorting", e.getCause());
                }
            }

            printMessage("Total vectors processed: " + vectorsToSort);

            // Sort chunk files by chunk number to maintain order
            chunkFiles.sort(Comparator.comparing(path -> {
                String name = path.getFileName().toString();
                // Extract chunk number from filename like "chunk_5.fvec"
                int underscoreIdx = name.indexOf('_');
                int dotIdx = name.indexOf('.');
                if (underscoreIdx >= 0 && dotIdx > underscoreIdx) {
                    return Integer.parseInt(name.substring(underscoreIdx + 1, dotIdx));
                }
                return 0;
            }));

            return chunkFiles;

        } finally {
            // Shutdown executor
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
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
    private <T> void mergeChunks(StatusScope sortScope, FileType fileType, Class<T> dataClass, List<Path> chunkFiles) throws IOException {
        // Create merge operation as a concrete task that implements StatusSource
        MergeOperation<T> mergeOp = new MergeOperation<>(fileType, dataClass, chunkFiles);

        // Track the merge operation and execute it
        try (io.nosqlbench.status.StatusTracker<MergeOperation<T>> tracker = sortScope.trackTask(mergeOp)) {
            mergeOp.execute();
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
