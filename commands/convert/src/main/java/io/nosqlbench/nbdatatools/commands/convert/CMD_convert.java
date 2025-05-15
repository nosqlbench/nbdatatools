package io.nosqlbench.nbdatatools.commands.convert;

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

import io.nosqlbench.nbvectors.api.fileio.VectorWriter;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.services.WriterLookup;
import io.nosqlbench.nbvectors.api.services.Selector;
import io.nosqlbench.readers.CsvJsonArrayStreamer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * # Vector Format Conversion Tool
 * 
 * This command converts vector data between different file formats:
 * 
 * ## Supported Modes
 * - Single file: Converts one file to another format
 * - Multiple files (combined): Converts multiple input files to a single output file
 * - Multiple files (parallel): Converts each input file to a separate output file in parallel
 *
 * - fvec: float vector format (4-byte float values)
 * - ivec: integer vector format (4-byte integer values)
 * - bvec: binary vector format (1-byte unsigned values)
 * - csv: comma-separated text format
 * - json: JSON-based vector format
 * 
 * # Basic Usage
 * ```
 * convert --input vectors.fvec --output vectors.csv
 * ```
 * 
 * # Data Flow
 * ```
 * ┌───────────────┐     ┌────────────────┐     ┌───────────────┐
 * │ Input         │────▶│ Optional       │────▶│ Output        │
 * │ Vector File   │     │ Transformations│     │ Vector File   │
 * └───────────────┘     └────────────────┘     └───────────────┘
 * ```
 * 
 * Formats are automatically detected by file extension.
 */
@Selector("convert")
@CommandLine.Command(name = "convert",
    header = "Convert between different vector file formats",
    description = """
        Converts vectors between different file formats including fvec, ivec, bvec, csv, and json.
        Formats are automatically detected from file extensions.""",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_convert implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_convert.class);
    
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_WARNING = 1;
    private static final int EXIT_ERROR = 2;
    
    /**
     * Callback interface for conversion progress and completion.
     */
    public interface ConversionCallback {
        void onSuccess(Path inputFile, Path outputFile, int vectorsProcessed);
        void onError(String message);
    }

    @CommandLine.Option(
        names = {"-i", "--input"},
        description = "Input vector file path (supports wildcards and fvec, ivec, bvec, csv, json formats)",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"-o", "--output"},
        description = "Output vector file path",
        required = true
    )
    private Path outputPath;

    @CommandLine.Option(
        names = {"--input-format"},
        description = "Explicitly specify input format (overrides file extension detection)"
    )
    private String inputFormat;

    @CommandLine.Option(
        names = {"--output-format"},
        description = "Explicitly specify output format (overrides file extension detection)"
    )
    private String outputFormat;

    @CommandLine.Option(
        names = {"-f", "--force"},
        description = "Force overwrite if output file already exists"
    )
    private boolean force = false;

    @CommandLine.Option(
        names = {"--normalize"},
        description = "Normalize vector magnitudes to 1.0 (L2 normalization)"
    )
    private boolean normalize = false;

    @CommandLine.Option(
        names = {"--precision"},
        description = "Set output precision for floating-point values (CSV/JSON formats)",
        defaultValue = "6"
    )
    private int precision = 6;

    @CommandLine.Option(
        names = {"--limit"},
        description = "Limit the number of vectors to convert"
    )
    private Integer limit;

    @CommandLine.Option(
        names = {"--offset"},
        description = "Start converting from this vector index (0-based)",
        defaultValue = "0"
    )
    private int offset = 0;

    @CommandLine.Option(
        names = {"-v", "--verbose"},
        description = "Enable verbose output"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"-q", "--quiet"},
        description = "Suppress all output except errors"
    )
    private boolean quiet = false;
    
    @CommandLine.Option(
        names = {"-p", "--parallel"},
        description = "Process multiple input files in parallel (each to its own output file)"
    )
    private boolean parallel = false;
    
    @CommandLine.Option(
        names = {"--threads"},
        description = "Number of processor threads per file conversion (default: number of available processors)",
        defaultValue = "0"
    )
    private int threads = 0;
    
    @CommandLine.Option(
        names = {"--max-parallel"},
        description = "Maximum number of files to process in parallel (default: number of available processors)",
        defaultValue = "0"
    )
    private int maxParallel = 0;
    
    @CommandLine.Option(
        names = {"--progress"},
        description = "Display progress updates (interval in milliseconds, 0 to disable)",
        defaultValue = "1000"
    )
    private int progressInterval = 1000;
    
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "Display this help message"
    )
    private boolean helpRequested = false;

    public static void main(String[] args) {
        CMD_convert cmd = new CMD_convert();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            // Resolve input path (may contain wildcards)
            List<Path> inputFiles = resolveWildcardPath(inputPath);
            if (inputFiles.isEmpty()) {
                logger.error("No input files found for: {}", inputPath);
                return EXIT_ERROR;
            }
            
            // Determine output format based on file extension or explicit option
            String detectedOutputFormat = outputFormat != null ? outputFormat : getFormatFromPath(outputPath);
            
            // Determine input format - use the same for all files if specified explicitly
            String detectedInputFormat;
            if (inputFormat != null) {
                detectedInputFormat = inputFormat;
            } else if (inputFiles.size() == 1) {
                detectedInputFormat = getFormatFromPath(inputFiles.get(0));
            } else {
                // For multiple files, check they all have the same format
                detectedInputFormat = getFormatFromPath(inputFiles.get(0));
                boolean formatsMismatch = false;
                
                for (int i = 1; i < inputFiles.size(); i++) {
                    String fileFormat = getFormatFromPath(inputFiles.get(i));
                    if (!fileFormat.equals(detectedInputFormat)) {
                        logger.warn("Mixed input formats: file {} has format {}, expected {}",
                            inputFiles.get(i), fileFormat, detectedInputFormat);
                        formatsMismatch = true;
                    }
                }
                
                if (formatsMismatch) {
                    logger.warn("Input files have different formats. Using {} for all files.", detectedInputFormat);
                    logger.warn("Specify --input-format explicitly to override.");
                }
            }
            
            // If there's more than one input file and parallel option is enabled,
            // process each file in parallel to its own output file
            if (parallel && inputFiles.size() > 1) {
                return convertFilesInParallel(inputFiles, detectedInputFormat, detectedOutputFormat);
            } else {
                // In non-parallel mode, create parent directories if needed
                Path parent = outputPath.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                
                if (verbose) {
                    logger.info("Converting from {} to {} using CsvJsonArrayStreamer and UniformFvecWriter", 
                        detectedInputFormat, detectedOutputFormat);
                    logger.info("Input: {} files from {}", inputFiles.size(), inputPath);
                    logger.info("Output: {}", outputPath);
                }
                
                // Run the conversion with multiple input files combined into one output
                return convertFloatVectors(inputFiles, detectedInputFormat, detectedOutputFormat);
            }
            
        } catch (Exception e) {
            logger.error("Error during conversion: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }
    
    /**
     * Determine the data type class based on file extension
     * 
     * @param fileExtension The file extension (e.g., "fvec", "ivec", "bvec", "dvec")
     * @return The appropriate data type class
     */
    private Class<?> inferDataTypeFromExtension(String fileExtension) {
        switch (fileExtension.toLowerCase()) {
            case "fvec":
                return float[].class;
            case "ivec":
            case "bvec":
                return int[].class;
            case "dvec":
                return double[].class;
            default:
                // Default to float[] for unknown extensions
                return float[].class;
        }
    }
    
    /**
     * Check if the format is a double vector format
     */
    private boolean isDoubleFormat(String format) {
        return format.equalsIgnoreCase("dvec");
    }
    
    /**
     * Retains the original single-file converter for backward compatibility
     */
    private Integer convertFloatVectors(String inputFormat, String outputFormat) throws Exception {
        return convertFloatVectors(Collections.singletonList(inputPath), inputFormat, outputFormat);
    }
    
    /**
     * Converts multiple files in parallel, each to its own output file.
     * 
     * @param inputFiles The list of input files to process
     * @param inputFormat The input file format
     * @param outputFormat The output file format
     * @return The exit code
     */
    private Integer convertFilesInParallel(List<Path> inputFiles, String inputFormat, String outputFormat) throws Exception {
        if (!force && Files.exists(outputPath) && !Files.isDirectory(outputPath)) {
            logger.error("Output path {} already exists and is not a directory. Use --force to overwrite.", outputPath);
            return EXIT_ERROR;
        }
        
        // Create output directory if it doesn't exist
        if (Files.isDirectory(outputPath) || outputPath.toString().endsWith(File.separator)) {
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }
        }
        
        if (verbose) {
            logger.info("Converting {} files in parallel mode from {} to {}", 
                inputFiles.size(), inputFormat, outputFormat);
            logger.info("Input: Files matching {}", inputPath);
            if (Files.isDirectory(outputPath)) {
                logger.info("Output: Files will be created in directory {}", outputPath);
            } else {
                logger.info("Output: Files will be created in same directory as input with extension {}", outputFormat);
            }
        }
        
        // Create a map to store the output paths for each input file
        Map<Path, Path> inputToOutputMap = new HashMap<>();
        
        // Generate output paths for each input file
        for (Path inputFile : inputFiles) {
            Path outputFile = generateOutputPath(inputFile, outputFormat);
            
            if (!force && Files.exists(outputFile)) {
                logger.warn("Output file already exists (skipping): {}", outputFile);
                continue;
            }
            
            inputToOutputMap.put(inputFile, outputFile);
        }
        
        if (inputToOutputMap.isEmpty()) {
            logger.error("No files to process (all output files exist). Use --force to overwrite.");
            return EXIT_ERROR;
        }
        
        // Determine number of parallel tasks
        int parallelTasks = maxParallel > 0 ? 
            Math.min(maxParallel, Runtime.getRuntime().availableProcessors()) : 
            Runtime.getRuntime().availableProcessors();
        
        if (verbose) {
            logger.info("Processing {} files using {} parallel tasks", 
                inputToOutputMap.size(), parallelTasks);
        }
        
        // Create thread pool for parallel conversion
        ExecutorService executorService = Executors.newFixedThreadPool(parallelTasks);
        
        // Track conversion results
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger totalVectors = new AtomicInteger(0);
        
        // Conversion callback
        ConversionCallback callback = new ConversionCallback() {
            @Override
            public synchronized void onSuccess(Path inputFile, Path outputFile, int vectorsProcessed) {
                successCount.incrementAndGet();
                totalVectors.addAndGet(vectorsProcessed);
            }
            
            @Override
            public synchronized void onError(String message) {
                logger.error(message);
                errorCount.incrementAndGet();
            }
        };
        
        // Create and submit conversion tasks
        List<Future<?>> futures = new ArrayList<>();
        
        for (Map.Entry<Path, Path> entry : inputToOutputMap.entrySet()) {
            Path inputFile = entry.getKey();
            Path outputFile = entry.getValue();
            
            SingleFileConverter converter = new SingleFileConverter(
                inputFile, outputFile, inputFormat, outputFormat,
                normalize, offset, limit, threads, verbose, quiet, callback
            );
            
            futures.add(executorService.submit(converter));
        }
        
        // Wait for all conversions to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.error("Error in conversion task: {}", e.getMessage());
                errorCount.incrementAndGet();
            }
        }
        
        // Shutdown the executor
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        
        // Show summary
        if (!quiet) {
            logger.info("Conversion complete: {} files processed successfully, {} errors, {} total vectors",
                successCount.get(), errorCount.get(), totalVectors.get());
        }
        
        return errorCount.get() > 0 ? EXIT_WARNING : EXIT_SUCCESS;
    }
    
    /**
     * Convert float vector data from multiple input files to a single output file using multiple threads.
     * Uses a single thread for reading and a single thread for writing, with multiple
     * worker threads for vector processing.
     * 
     * @param inputFiles List of input files to process
     * @param inputFormat Format of the input files
     * @param outputFormat Format of the output file
     * @return Exit code
     */
    private Integer convertFloatVectors(List<Path> inputFiles, String inputFormat, String outputFormat) throws Exception {
        if (Files.exists(outputPath) && !force) {
            logger.error("Output file already exists. Use --force to overwrite.");
            return EXIT_ERROR;
        }
        
        // First, read the first vector to determine dimension
        if (inputFiles.isEmpty()) {
            logger.error("No input files to process");
            return EXIT_ERROR;
        }
        
        Path firstPath = inputFiles.get(0);
        CsvJsonArrayStreamer firstStreamer = new CsvJsonArrayStreamer(firstPath);
        Iterator<float[]> firstIterator = firstStreamer.iterator();
        
        if (!firstIterator.hasNext()) {
            logger.error("First input file is empty: {}", firstPath);
            return EXIT_ERROR;
        }
        
        // Get the vector dimension from the first file
        float[] firstVector = firstIterator.next();
        int dimension = firstVector.length;
        // Note: CsvJsonArrayStreamer does not implement Closeable; remove explicit close to avoid compile error
        // firstStreamer.close();

        if (verbose) {
            logger.info("Detected vector dimension: {}", dimension);
        }
        
        // Create a multi-file iterator for all input files
        MultiFileVectorIterator multiFileIterator = new MultiFileVectorIterator(inputFiles, dimension, verbose);
        
        // Create and use UniformFvecWriter directly for output
        VectorWriter<float[]> writer =
            WriterLookup.findWriter(Encoding.Type.xvec, float[].class).orElseThrow();

        // Determine the number of processor threads to use
        int threadCount = (threads > 0) ? threads : Runtime.getRuntime().availableProcessors();
        
        if (verbose) {
            logger.info("Using UniformFvecWriter for output with dimension {}", dimension);
            logger.info("Using {} processor threads", threadCount);
        }
        
        try {
            // Skip initial vectors if offset > 0
            for (int i = 0; i < offset && multiFileIterator.hasNext(); i++) {
                multiFileIterator.next();
            }
            
            // Create thread-safe queues for the vectors
            BlockingQueue<float[]> processingQueue = new LinkedBlockingQueue<>(1000);
            BlockingQueue<float[]> writingQueue = new LinkedBlockingQueue<>(1000);
            
            // Flags and counters for coordination
            AtomicBoolean readerDone = new AtomicBoolean(false);
            AtomicBoolean processorError = new AtomicBoolean(false);
            AtomicInteger activeProcessors = new AtomicInteger(threadCount);
            
            // Initialize count estimate based on limit or file size
            Integer estimatedTotal = limit;
            
            // If no limit is specified, try to make an initial estimate based on file size
            if (estimatedTotal == null) {
                long totalFileSize = 0;
                for (Path file : inputFiles) {
                    try {
                        totalFileSize += Files.size(file);
                    } catch (IOException e) {
                        logger.warn("Could not determine size of {}: {}", file, e.getMessage());
                    }
                }
                
                if (verbose) {
                    logger.info("Total input file size: {} bytes", totalFileSize);
                }
                
                // Very rough initial estimate based on dimension and file size
                // This will be refined during processing
                if (totalFileSize > 0) {
                    long bytesPerVector = 4L * dimension; // 4 bytes per float * dimension
                    long initialEstimate = totalFileSize / bytesPerVector;
                    
                    // Cap the estimate at Integer.MAX_VALUE
                    if (initialEstimate < Integer.MAX_VALUE) {
                        estimatedTotal = (int) initialEstimate;
                    }
                }
            }
            
            // Create progress tracker with initial estimate
            ConversionProgress progress = new ConversionProgress(estimatedTotal, 0);
            
            // Create thread pool for processing
            ExecutorService processorPool = Executors.newFixedThreadPool(threadCount);
            
            // Start progress display if enabled and not in quiet mode
            ScheduledExecutorService progressUpdater = null;
            if (progressInterval > 0 && !quiet) {
                progressUpdater = Executors.newSingleThreadScheduledExecutor();
                progressUpdater.scheduleAtFixedRate(
                    progress::displayProgress,
                    progressInterval, progressInterval, TimeUnit.MILLISECONDS
                );
                
                // Initial progress display to show we've started
                progress.displayProgress();
            }
            
            // Start the reader thread
            Thread readerThread = new Thread(() -> {
                try {
                    int count = 0;
                    int lastEstimationUpdate = 0;
                    while (multiFileIterator.hasNext() && (limit == null || count < limit)) {
                        float[] vector = multiFileIterator.next();
                        processingQueue.put(vector); // This will block if queue is full
                        count++;
                        
                        // Update estimation occasionally based on vector count
                        if (count - lastEstimationUpdate > 1000) { // Every 1000 vectors
                            progress.updateEstimationFromCount(count);
                            lastEstimationUpdate = count;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error in reader thread: {}", e.getMessage());
                    processorError.set(true);
                } finally {
                    readerDone.set(true);
                }
            });
            readerThread.start();
            
            // Start the processor threads
            for (int i = 0; i < threadCount; i++) {
                processorPool.submit(() -> {
                    try {
                        while (!readerDone.get() || !processingQueue.isEmpty()) {
                            float[] vector = processingQueue.poll(100, TimeUnit.MILLISECONDS);
                            if (vector != null) {
                                if (normalize) {
                                    normalizeVector(vector);
                                }
                                writingQueue.put(vector);
                                progress.incrementProcessed();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error in processor thread: {}", e.getMessage());
                        processorError.set(true);
                    } finally {
                        activeProcessors.decrementAndGet();
                    }
                });
            }
            
            // Start the writer thread
            Thread writerThread = new Thread(() -> {
                try {
                    int writtenVectors = 0;
                    while (activeProcessors.get() > 0 || !writingQueue.isEmpty()) {
                        float[] vector = writingQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (vector != null) {
                            writer.write(vector);
                            writtenVectors++;
                        }
                    }
                    if (verbose) {
                        logger.info("Writer finished. Wrote {} vectors", writtenVectors);
                    }
                } catch (Exception e) {
                    logger.error("Error in writer thread: {}", e.getMessage());
                    processorError.set(true);
                }
            });
            writerThread.start();
            
            // Wait for all threads to complete
            readerThread.join();
            processorPool.shutdown();
            processorPool.awaitTermination(1, TimeUnit.HOURS);
            writerThread.join();
            
            // Shutdown progress display
            if (progressUpdater != null) {
                progressUpdater.shutdown();
                progressUpdater.awaitTermination(1, TimeUnit.SECONDS);
            }
            
            // Display final progress with a newline to clear the progress line
            if (!quiet) {
                System.out.println();
                progress.displayFinalProgress();
            }
            
            if (processorError.get()) {
                logger.error("Errors occurred during processing");
                return EXIT_ERROR;
            }
            
            return EXIT_SUCCESS;
                    } finally {
            // Ensure resources are closed even if exceptions occur
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing writer: {}", e.getMessage());
            }
            
            try {
                if (multiFileIterator != null) {
                    multiFileIterator.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing iterator: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Convert integer vector data from input to output format
     */
    private Integer convertIntVectors(String inputFormat, String outputFormat) throws Exception {
        // Currently only supporting float vectors with UniformFvecWriter
        logger.error("Integer vector conversion is not supported in the direct implementation mode");
        return EXIT_ERROR;
    }
    
    /**
     * Normalize a vector to unit length (L2 normalization)
     * This method is thread-safe as it only operates on the provided vector.
     * 
     * @param vector The vector to normalize
     */
    private void normalizeVector(float[] vector) {
        // Calculate vector magnitude (L2 norm)
        double sumSquares = 0.0;
        for (float v : vector) {
            sumSquares += v * v;
        }
        double magnitude = Math.sqrt(sumSquares);
        
        // Skip normalization if magnitude is zero or very close to zero
        if (magnitude < 1e-10) {
            return;
        }
        
        // Normalize each component
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) (vector[i] / magnitude);
        }
    }
    
    /**
     * Extract format name from file path extension
     * 
     * @param path The file path to extract the format from
     * @return The format string derived from the file extension
     */
    private String getFormatFromPath(Path path) {
        String filename = path.getFileName().toString().toLowerCase();
        int dotIndex = filename.lastIndexOf('.');
        
        if (dotIndex > 0 && dotIndex < filename.length() - 1) {
            return filename.substring(dotIndex + 1);
        }
        
        // Default to fvec if no extension
        return "fvec";
    }
    
    /**
     * Check if the format is a float vector format
     */
    private boolean isFloatFormat(String format) {
        return format.equals("fvec") || format.equals("csv") || format.equals("json");
    }
    
    /**
     * Check if the format is an integer vector format
     */
    private boolean isIntFormat(String format) {
        return format.equals("ivec") || format.equals("bvec");
    }
    
    /**
     * Generate an output path for a given input path using the specified output format.
     * Preserves the filename but changes the extension.
     * 
     * @param inputPath The input file path
     * @param outputFormat The desired output format (extension)
     * @return The generated output path
     */
    private Path generateOutputPath(Path inputPath, String outputFormat) {
        String filename = inputPath.getFileName().toString();
        int dotIndex = filename.lastIndexOf('.');
        
        String baseName;
        if (dotIndex > 0) {
            baseName = filename.substring(0, dotIndex);
        } else {
            baseName = filename;
        }
        
        String outputFileName = baseName + "." + outputFormat;
        
        // If output path specified is a directory, use it as the parent
        if (Files.isDirectory(outputPath)) {
            return outputPath.resolve(outputFileName);
        }
        
        // If output path doesn't exist and ends with a separator, treat as directory
        String outputPathStr = outputPath.toString();
        if (outputPathStr.endsWith(File.separator)) {
            // Create the directory if it doesn't exist
            try {
                Files.createDirectories(outputPath);
                return outputPath.resolve(outputFileName);
            } catch (IOException e) {
                logger.warn("Could not create directory {}, using parent of input", outputPath);
            }
        }
        
        // Otherwise use the parent of the input file and the output format extension
        return inputPath.getParent() != null 
            ? inputPath.getParent().resolve(outputFileName)
            : Path.of(outputFileName);
    }
    
    /**
     * Resolves a path that may contain wildcards into a list of matching files.
     * 
     * @param path The input path that may contain wildcards
     * @return A list of files that match the pattern
     * @throws IOException If there's an error accessing the filesystem
     */
    private List<Path> resolveWildcardPath(Path path) throws IOException {
        String pathStr = path.toString();
        
        // If no wildcards, just return the path as-is if it exists
        if (!pathStr.contains("*") && !pathStr.contains("?")) {
            if (Files.exists(path)) {
                return Collections.singletonList(path);
            } else {
                logger.error("Input file does not exist: {}", path);
                return Collections.emptyList();
            }
        }
        
        // Get the parent directory (up to the last directory before the wildcard)
        Path parentDir = null;
        String pattern = null;
        int wildcardIndex = Math.min(
            pathStr.lastIndexOf('*') != -1 ? pathStr.lastIndexOf('*') : Integer.MAX_VALUE,
            pathStr.lastIndexOf('?') != -1 ? pathStr.lastIndexOf('?') : Integer.MAX_VALUE
        );
        
        int lastSeparatorBeforeWildcard = pathStr.lastIndexOf(File.separator, wildcardIndex);
        if (lastSeparatorBeforeWildcard >= 0) {
            parentDir = Path.of(pathStr.substring(0, lastSeparatorBeforeWildcard));
            pattern = pathStr.substring(lastSeparatorBeforeWildcard + 1);
        } else {
            parentDir = Path.of(".");
            pattern = pathStr;
        }
        
        if (!Files.exists(parentDir)) {
            logger.error("Parent directory does not exist: {}", parentDir);
            return Collections.emptyList();
        }
        
        if (!Files.isDirectory(parentDir)) {
            logger.error("Parent path is not a directory: {}", parentDir);
            return Collections.emptyList();
        }
        
        // Create a glob pattern for matching files
        String globPattern = "glob:" + pattern;
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(globPattern);
        
        // Find matching files
        List<Path> matchedPaths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(parentDir)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry) && matcher.matches(entry.getFileName())) {
                    matchedPaths.add(entry);
                }
            }
        }
        
        if (matchedPaths.isEmpty()) {
            logger.warn("No files match the pattern: {}", path);
        } else if (verbose) {
            logger.info("Found {} files matching {}", matchedPaths.size(), path);
            if (matchedPaths.size() <= 10 || verbose) {
                for (Path matchedPath : matchedPaths) {
                    logger.info("  - {}", matchedPath);
                }
            }
        }
        
        // Sort files to ensure consistent processing order
        Collections.sort(matchedPaths);
        return matchedPaths;
    }
}
