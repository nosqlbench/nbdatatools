package io.nosqlbench.nbvectors.commands.generate.commands;

import io.nosqlbench.readers.UniformFvecReader;
import io.nosqlbench.readers.UniformIvecReader;
import io.nosqlbench.xvec.writers.FvecVectorWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.Display;
import org.jline.utils.InfoCmp;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 Extract vectors from an fvec file using indices from an ivec file.

 This extraction process supports parallel processing with configurable thread count
 and memory usage limits to balance performance with resource constraints.

 The extraction process includes the following safeguards and optimizations:
 - Once the IvecReader is opened, the minimum and maximum range values are accessed to assert
 a valid range. This ensures a fail-fast check occurs before a long process that might fail later.
 - The extraction process can run concurrently using multiple threads for better performance.
 - The user can specify the number of threads using the --threads option.
 - An advisory limit on the amount of buffer memory can be set using the --memory-limit option.
 - Range values are validated to ensure they are positive and within bounds of the ivec file.
 - If any index value refers to a position that does not exist in the fvec file, an error is thrown. */
@CommandLine.Command(name = "fvec-extract",
    description = "Extract vectors from a fvec file using indices from an ivec file")
public class FvecExtract implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(FvecExtract.class);

  // JLine terminal display components
  private Terminal terminal;
  private Display display;
  private final List<AttributedString> displayLines = new ArrayList<>();
  private final AtomicBoolean displayActive = new AtomicBoolean(false);
  private Thread shutdownHook;

  @CommandLine.Option(names = {"--ivec-file"},
      description = "Path to ivec file containing indices",
      required = true)
  private String ivecFile;

  @CommandLine.Option(names = {"--range"},
      description = "Range of indices to use (format: start..end), e.g. 0..1000 or 2000..3000",
      required = true)
  private String range;

  @CommandLine.Option(names = {"--fvec-file"},
      description = "Path to fvec file containing vectors to extract",
      required = true)
  private String fvecFile;

  @CommandLine.Option(names = {"--output"},
      description = "Path to output fvec file",
      required = true)
  private String outputFile;

  @CommandLine.Option(names = {"--force"},
      description = "Force overwrite of output file if it exists",
      defaultValue = "false")
  private boolean force;

  @CommandLine.Option(names = {"--threads"},
      description = "Number of threads to use for parallel processing (0 = use available processors)",
      defaultValue = "0")
  private int threads;

  @CommandLine.Option(names = {"--chunk-size"},
      description = "Size of chunks for parallel processing",
      defaultValue = "1000")
  private int chunkSize;

  @CommandLine.Option(names = {"--memory-limit"},
      description = "Advisory limit on total buffer memory to use in MB (0 = no limit)",
      defaultValue = "0")
  private int memoryLimitMB;

  /// Parse a range string in the format "start..end" into a long array with \[start, end] values
  /// @param rangeStr
  ///     String in format "start..end"
  /// @return long array with \[startIndex, endIndex]
  /// @throws IllegalArgumentException
  ///     if the range format is invalid
  private long[] parseRange(String rangeStr) {
    if (rangeStr == null || !rangeStr.contains("..")) {
      throw new IllegalArgumentException("Range must be in format 'start..end'");
    }

    String[] parts = rangeStr.split("\\.\\.");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Range must be in format 'start..end'");
    }

    try {
      long start = Long.parseLong(parts[0]);
      long end = Long.parseLong(parts[1]);

      if (start < 0 || end < 0) {
        throw new IllegalArgumentException("Range values cannot be negative");
      }

      return new long[]{start, end};
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Range values must be valid numbers", e);
    }
  }

  /// Calculates memory requirements for vectors
  /// @param dimension
  ///     The dimension of each vector
  /// @param count
  ///     The number of vectors
  /// @return Estimated memory requirement in bytes
  private long calculateMemoryRequirement(int dimension, long count) {
    // Each float is 4 bytes
    // Add some overhead for object headers and references
    return count * (dimension * 4L + 16);
  }

  /// Initialize the JLine terminal display
  /// @return true if successful, false otherwise
  private boolean initializeTerminal() {
    try {
      // Create a terminal instance
      terminal = TerminalBuilder.builder().name("FvecExtract").system(true).jansi(true).build();

      // Create a display for updating the terminal
      display = new Display(terminal, false); // Use false for non-full-screen mode
      displayActive.set(true);

      // Initialize display lines
      displayLines.clear();
      for (int i = 0; i < 8; i++) {
        displayLines.add(new AttributedString(""));
      }

      // Clear the screen and make sure the cursor is at the beginning
      terminal.puts(InfoCmp.Capability.clear_screen);
      terminal.puts(InfoCmp.Capability.carriage_return);
      terminal.flush();

      logger.info("JLine terminal initialized successfully");
      return true;
    } catch (IOException e) {
      logger.warn(
          "Failed to initialize JLine terminal: {}. Falling back to simple progress display.",
          e.getMessage()
      );
      return false;
    }
  }

  /// Properly shut down the display
  private void shutdownDisplay() {
    if (displayActive.get()) {
      try {
        // Clear any existing content from the display
        if (display != null) {
          display.clear();
        }
        
        if (terminal != null) {
          // Show cursor if it was hidden
          terminal.puts(InfoCmp.Capability.cursor_normal);
          
          // Make sure the cursor is positioned at a good spot
          terminal.puts(InfoCmp.Capability.carriage_return);
          
          // Add some space after the display
          terminal.writer().println();
          terminal.writer().println("Vector extraction completed.");
          terminal.writer().println();
          
          // Flush all output
          terminal.flush();
        }
        
        // Mark display as inactive
        displayActive.set(false);
        
        logger.info("Display shutdown successfully");
      } catch (Exception e) {
        logger.warn("Error during display shutdown: {}", e.getMessage());
      }
    }
  }
  
  /// Cleanup the terminal resources
  private void cleanupTerminal() {
    if (terminal != null) {
      try {
        // Make sure display is shut down first
        shutdownDisplay();
  
        // Close the terminal properly
        try {
          terminal.close();
        } catch (Exception e) {
          logger.warn("Error closing terminal: {}", e.getMessage());
        }
  
        logger.info("Terminal display cleaned up and closed");
      } catch (Exception e) {
        logger.warn("Error cleaning up terminal display: {}", e.getMessage());
      } finally {
        // Ensure references are cleared
        display = null;
        terminal = null;
      }
    }
  }

  /// Display progress information for the extraction process using JLine
  /// @param processed
  ///     Number of vectors processed so far
  /// @param total
  ///     Total number of vectors to process
  /// @param startTime
  ///     Time when the extraction started
  /// @param bufferMemory
  ///     Current memory usage by buffers (0 if not tracked)
  private void displayProgress(long processed, long total, Instant startTime, long bufferMemory) {
    if (!displayActive.get()) {
      // Fall back to simple progress if terminal display isn't active
      displaySimpleProgress(processed, total, startTime, bufferMemory);
      return;
    }

    Instant now = Instant.now();
    Duration elapsed = Duration.between(startTime, now);

    // Calculate processing rate (vectors per second)
    double vectorsPerSecond = processed / Math.max(0.001, elapsed.toMillis() / 1000.0);

    // Calculate percentage and other statistics
    double percentage = (double) processed / total * 100;
    long remainingSeconds =
        (processed > 0 && vectorsPerSecond > 0) ? (long) ((total - processed) / vectorsPerSecond) :
            0;
    long elapsedSeconds = elapsed.getSeconds();

    try {
      // Clear the display lines
      displayLines.clear();

      // Get terminal dimensions
      int width = Math.max(80, terminal.getWidth());
      int height = Math.max(20, terminal.getHeight());
      int barWidth = Math.min(50, width - 10); // Ensure bar fits in terminal

      // Title with styled text as AttributedString
      AttributedString title =
          new AttributedStringBuilder().style(AttributedStyle.BOLD.foreground(AttributedStyle.BLUE))
              .append("FVEC EXTRACTION PROGRESS").toAttributedString();
      displayLines.add(title);

      // Separator as AttributedString
      displayLines.add(new AttributedString("─".repeat(Math.min(width, 80))));

      // Status line with progress percentage
      String statusText =
          String.format("Processed: %,d of %,d vectors [%.1f%%]", processed, total, percentage);
      displayLines.add(new AttributedString(statusText));

      // Timing information
      String timingText = String.format(
          "Speed: %.1f vectors/sec | Elapsed: %02d:%02d:%02d | ETA: %02d:%02d:%02d",
          vectorsPerSecond,
          elapsedSeconds / 3600,
          (elapsedSeconds % 3600) / 60,
          elapsedSeconds % 60,
          remainingSeconds / 3600,
          (remainingSeconds % 3600) / 60,
          remainingSeconds % 60
      );
      displayLines.add(new AttributedString(timingText));

      // Progress bar
      StringBuilder barLine = new StringBuilder();
      barLine.append("[");
      int completedWidth = (int) ((processed * barWidth) / (double) total);

      for (int i = 0; i < barWidth; i++) {
        if (i < completedWidth) {
          barLine.append("=");
        } else if (i == completedWidth) {
          barLine.append(">");
        } else {
          barLine.append(" ");
        }
      }
      barLine.append("]");
      displayLines.add(new AttributedString(barLine.toString()));

      // Memory usage if limit is specified
      if (memoryLimitMB > 0) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        double usedMemoryMB = usedMemory / (1024.0 * 1024.0);
        double memoryPercentage = (usedMemoryMB / memoryLimitMB) * 100;

        // Include buffer memory information if available
        String bufferInfo = "";
        if (bufferMemory > 0) {
          double bufferMemoryMB = bufferMemory / (1024.0 * 1024.0);
          bufferInfo = String.format(" (Buffer: %.1f MB)", bufferMemoryMB);
        }

        String memoryText = String.format(
            "Memory: %.1f MB / %d MB [%.1f%%]%s",
            usedMemoryMB,
            memoryLimitMB,
            memoryPercentage,
            bufferInfo
        );
        displayLines.add(new AttributedString(memoryText));

        // Memory bar
        StringBuilder memBarLine = new StringBuilder();
        memBarLine.append("[");
        int memCompletedWidth = (int) ((usedMemoryMB * barWidth) / memoryLimitMB);

        for (int i = 0; i < barWidth; i++) {
          if (i < memCompletedWidth) {
            if (memoryPercentage > 90) {
              // Red for high memory usage
              memBarLine.append("#");
            } else if (memoryPercentage > 70) {
              // Amber for medium memory usage
              memBarLine.append("*");
            } else {
              memBarLine.append("=");
            }
          } else {
            memBarLine.append(" ");
          }
        }
        memBarLine.append("]");
        displayLines.add(new AttributedString(memBarLine.toString()));
      }

      // Thread status info
      String threadInfoText = String.format(
          "Using %d threads with chunk size %d",
          threads > 0 ? threads : Runtime.getRuntime().availableProcessors(),
          chunkSize
      );
      displayLines.add(new AttributedString(threadInfoText));

      // Ensure we have the right number of lines
      while (displayLines.size() < 10) {
        displayLines.add(new AttributedString(""));
      }

      // Update the display with appropriate dimensions
      display.resize(height, width);
      display.update(displayLines, 0); // Using 0 so cursor is at the top
      terminal.flush(); // Explicitly flush the terminal
    } catch (Exception e) {
      // If any error occurs with JLine display, disable it and fall back to simple display
      logger.warn(
          "Error updating JLine display: {}. Falling back to simple progress.",
          e.getMessage(),
          e
      );
      displayActive.set(false);
      displaySimpleProgress(processed, total, startTime, bufferMemory);
    }
  }

  /// Fallback method for simple progress display without JLine
  /// @param processed
  ///     Number of vectors processed so far
  /// @param total
  ///     Total number of vectors to process
  /// @param startTime
  ///     Time when the extraction started
  /// @param bufferMemory
  ///     Current memory usage by buffers (0 if not tracked)
  private void displaySimpleProgress(
      long processed,
      long total,
      Instant startTime,
      long bufferMemory
  )
  {
    Instant now = Instant.now();
    Duration elapsed = Duration.between(startTime, now);

    // Calculate processing rate (vectors per second)
    double vectorsPerSecond = processed / Math.max(0.001, elapsed.toMillis() / 1000.0);

    // Clear the current line
    System.out.print("\r");

    StringBuilder progress = new StringBuilder();
    progress.append("Extracting: ").append(processed).append("/").append(total);

    // Add percentage
    double percentage = (double) processed / total * 100;
    progress.append(String.format(" [%.1f%%]", percentage));

    // Add estimated time remaining
    if (processed > 0 && vectorsPerSecond > 0) {
      long remainingSeconds = (long) ((total - processed) / vectorsPerSecond);
      progress.append(String.format(
          ", ETA: %02d:%02d:%02d",
          remainingSeconds / 3600,
          (remainingSeconds % 3600) / 60,
          remainingSeconds % 60
      ));
    }

    // Add rate information
    progress.append(String.format(" | %.1f vectors/sec", vectorsPerSecond));

    // Add elapsed time
    long elapsedSeconds = elapsed.getSeconds();
    progress.append(String.format(
        " | Time: %02d:%02d:%02d",
        elapsedSeconds / 3600,
        (elapsedSeconds % 3600) / 60,
        elapsedSeconds % 60
    ));

    // Add buffer memory information if available
    if (bufferMemory > 0) {
      double bufferMemoryMB = bufferMemory / (1024.0 * 1024.0);
      progress.append(String.format(" | Buffer: %.1f MB", bufferMemoryMB));
    }

    // Output progress directly to stdout
    System.out.print(progress.toString());
    System.out.flush();
  }

  @Override
  public Integer call() throws Exception {
    // Initialize the JLine terminal
    boolean terminalInitialized = false;
    
    // Add a JVM shutdown hook to ensure terminal is closed properly
    shutdownHook = new Thread(() -> {
      logger.info("JVM shutdown hook triggered, cleaning up terminal");
      cleanupTerminal();
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    
    try {
      terminalInitialized = initializeTerminal();

      Path ivecPath = Paths.get(ivecFile);
      Path fvecPath = Paths.get(fvecFile);
      Path outputPath = Paths.get(outputFile);

      // Check if input files exist
      if (!Files.exists(ivecPath)) {
        logger.error("Ivec file does not exist: {}", ivecPath);
        return 1;
      }

      if (!Files.exists(fvecPath)) {
        logger.error("Fvec file does not exist: {}", fvecPath);
        return 1;
      }

      // Check if output file exists and handle accordingly
      if (Files.exists(outputPath)) {
        if (!force) {
          logger.error("Output file already exists: {}. Use --force to overwrite.", outputPath);
          return 1;
        }
        logger.warn("Overwriting existing file: {}", outputPath);
      }

      UniformIvecReader ivecReader = null;
      UniformFvecReader fvecReader = null;
      FvecVectorWriter fvecWriter = null;

      try {
        // Open the readers
        ivecReader = new UniformIvecReader();
        ivecReader.open(ivecPath);

        // Get the sizes
        int ivecSize = ivecReader.getSize();
        int fvecSize = fvecReader.getSize();
        int dimension = fvecReader.getDimension();

        // Validate the range
        long[] rangeBounds = parseRange(range);
        long startIndex = rangeBounds[0];
        long endIndex = rangeBounds[1];

        // Ensure range is within bounds
        if (startIndex < 0) {
          logger.error("Start index must be non-negative: {}", startIndex);
          return 1;
        }

        if (endIndex >= ivecSize) {
          logger.warn(
              "End index {} exceeds ivec size {}. Will only process up to index {}",
              endIndex,
              ivecSize,
              ivecSize - 1
          );
          endIndex = ivecSize - 1;
        }

        if (startIndex > endIndex) {
          logger.error("Start index {} is greater than end index {}", startIndex, endIndex);
          return 1;
        }

        // Fail-fast check: access the minimum and maximum indices in the range to verify validity
        try {
          // Check the first index in the range
          int[] firstIndexVector = ivecReader.get((int) startIndex);
          int firstIndex = firstIndexVector[0];
          if (firstIndex < 0 || firstIndex >= fvecSize) {
            logger.error(
                "The first index {} in range (at position {}) is out of bounds for fvec file (size: {})",
                firstIndex,
                startIndex,
                fvecSize
            );
            return 1;
          }

          // Check the last index in the range
          int[] lastIndexVector = ivecReader.get((int) endIndex);
          int lastIndex = lastIndexVector[0];
          if (lastIndex < 0 || lastIndex >= fvecSize) {
            logger.error(
                "The last index {} in range (at position {}) is out of bounds for fvec file (size: {})",
                lastIndex,
                endIndex,
                fvecSize
            );
            return 1;
          }

          // Sample a few indices in between for better validation
          if (endIndex - startIndex > 10) {
            // Sample some intermediate points if range is large enough
            long midIndex = startIndex + (endIndex - startIndex) / 2;
            int[] midIndexVector = ivecReader.get((int) midIndex);
            int midValue = midIndexVector[0];
            if (midValue < 0 || midValue >= fvecSize) {
              logger.error(
                  "Middle index {} in range (at position {}) is out of bounds for fvec file (size: {})",
                  midValue,
                  midIndex,
                  fvecSize
              );
              return 1;
            }
          }

          logger.info(
              "Range validation successful: first index={}, last index={}",
              firstIndex,
              lastIndex
          );
        } catch (Exception e) {
          logger.error("Failed to validate index range: {}", e.getMessage(), e);
          return 1;
        }

        // Calculate number of vectors to extract
        long vectorCount = endIndex - startIndex + 1;

        // Create output file with writer
        fvecWriter = new FvecVectorWriter();
        fvecWriter.open(outputPath);

        // Determine number of threads to use
        int threadCount = threads;
        if (threadCount <= 0) {
          threadCount = Runtime.getRuntime().availableProcessors();
        }

        // Use at least 1 thread, but don't use more threads than we have vectors
        final int finalThreadCount = Math.max(1, Math.min(threadCount, (int) vectorCount));

        // Create a global AtomicLong for buffer memory tracking
        final AtomicLong totalBufferMemory = new AtomicLong(0);

        if (memoryLimitMB > 0) {
          logger.info(
              "Extracting {} vectors from {} using indices from {} (range: {}..{}) with {} threads and ~{} MB memory limit",
              vectorCount,
              fvecPath,
              ivecPath,
              startIndex,
              endIndex,
              finalThreadCount,
              memoryLimitMB
          );
        } else {
          logger.info(
              "Extracting {} vectors from {} using indices from {} (range: {}..{}) with {} threads",
              vectorCount,
              fvecPath,
              ivecPath,
              startIndex,
              endIndex,
              finalThreadCount
          );
        }

        // Setup progress tracking with thread-safe counter
        Instant startTime = Instant.now();
        final AtomicLong processedVectors = new AtomicLong(0);

        // Initial status message to stdout
        System.out.println("Starting vector extraction with " + finalThreadCount + " threads...");

        // Create a dedicated thread for progress reporting
        Thread progressThread = null;
        if (displayActive.get()) {
          progressThread = new Thread(() -> {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                // Pass buffer memory usage to display if we're tracking it
                long bufferMemory = finalThreadCount > 1 ? totalBufferMemory.get() : 0;
                displayProgress(processedVectors.get(), vectorCount, startTime, bufferMemory);
                Thread.sleep(200); // Update display 5 times per second
              }
            } catch (InterruptedException e) {
              // Thread was interrupted, which is expected when we're done
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              logger.warn("Error in progress reporting thread: {}", e.getMessage());
            }
          });
          progressThread.setDaemon(true);
          progressThread.start();
          logger.info("Started progress reporting thread");
        }
        
        // Create a dedicated memory monitor thread if memory limit is set
        Thread memoryMonitorThread = null;
        if (memoryLimitMB > 0 && finalThreadCount > 1) {
          memoryMonitorThread = new Thread(() -> {
            try {
              while (!Thread.currentThread().isInterrupted()) {
                // Check current memory usage
                long bufferMemory = totalBufferMemory.get();
                long memoryLimitBytes = memoryLimitMB * 1024L * 1024L;
                
                // If we're using more than 90% of our limit, log a warning and suggest GC
                if (bufferMemory > memoryLimitBytes * 0.9) {
                  logger.warn("Buffer memory usage ({}MB) exceeds 90% of limit ({}MB)",
                      bufferMemory / (1024.0 * 1024.0), memoryLimitMB);
                  System.gc();
                }
                
                Thread.sleep(1000); // Check memory once per second
              }
            } catch (InterruptedException e) {
              // Thread was interrupted, which is expected when we're done
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              logger.warn("Error in memory monitor thread: {}", e.getMessage());
            }
          });
          memoryMonitorThread.setDaemon(true);
          memoryMonitorThread.start();
          logger.info("Started memory monitor thread");
        }

        // Show initial progress display
        if (displayActive.get()) {
          displayProgress(0, vectorCount, startTime, 0);
        }

        // Concurrent extraction implementation
        if (finalThreadCount > 1) {
          // Create a thread pool
          ExecutorService executor = Executors.newFixedThreadPool(finalThreadCount);

          try {
            // Calculate memory-efficient chunk size if memory limit is specified
            int actualChunkSize = chunkSize;
            int estimatedChunkCount =
                finalThreadCount * 2; // Start with at least 2 chunks per thread

            if (memoryLimitMB > 0) {
              // Calculate bytes per vector (dimension * 4 bytes per float + small overhead)
              long bytesPerVector = dimension * 4L + 16; // 4 bytes per float + overhead

              // Calculate total allowed memory in bytes (convert MB to bytes)
              long memoryLimitBytes = memoryLimitMB * 1024L * 1024L;

              // Calculate max vectors that can fit in memory with this limit
              long maxVectorsInMemory = memoryLimitBytes / bytesPerVector;

              // Ensure we have at least one vector per thread, regardless of memory limit
              maxVectorsInMemory = Math.max(maxVectorsInMemory, finalThreadCount);

              // Calculate chunk size based on memory limit
              int memoryLimitedChunkSize =
                  (int) Math.max(1, maxVectorsInMemory / estimatedChunkCount);

              // Use the lesser of the specified chunk size and memory-limited chunk size
              actualChunkSize = Math.min(actualChunkSize, memoryLimitedChunkSize);

              logger.info(
                  "Memory limit: {} MB, adjusted chunk size to {} vectors per chunk",
                  memoryLimitMB,
                  actualChunkSize
              );
            }

            // Ensure chunk size is at least 1 and not more than the total vector count
            actualChunkSize = Math.max(1, Math.min(actualChunkSize, (int) vectorCount));

            // Use concurrent collections for thread safety
            final List<CompletableFuture<Void>> futures = new ArrayList<>();
            // Buffer collections that need to be cleared after use to prevent memory leaks
            final List<float[][]> chunkBuffers = new ArrayList<>();
            final List<int[]> chunkIndices = new ArrayList<>();

            // Create a lock for synchronizing critical sections if needed
            Lock progressLock = new ReentrantLock();

            // Process in chunks
            for (long chunkStart = startIndex;
                 chunkStart <= endIndex; chunkStart += actualChunkSize) {
              final long cs = chunkStart;
              final long ce = Math.min(endIndex, chunkStart + actualChunkSize - 1);
              final int chunkIndex = chunkBuffers.size();

              // Pre-allocate buffer for this chunk's vectors and indices
              final int chunkLength = (int) (ce - cs + 1);
              final float[][] vectorBuffer = new float[chunkLength][];
              final int[] indexBuffer = new int[chunkLength];
              chunkBuffers.add(vectorBuffer);
              chunkIndices.add(indexBuffer);

              // Calculate and track estimated buffer memory (approximate)
              // Vector buffer array overhead + index buffer size
              long bufferOverhead = chunkLength * 8L + chunkLength * 4L;
              totalBufferMemory.addAndGet(bufferOverhead);

              // Capture effectively final references to shared objects
              final UniformIvecReader ivecReaderFinal = ivecReader;
              final UniformFvecReader fvecReaderFinal = fvecReader;
              final AtomicLong processedVectorsFinal = processedVectors;

              // Submit the chunk for processing
              FvecVectorWriter finalFvecWriter = fvecWriter;
              CompletableFuture<Void> future = CompletableFuture.runAsync(
                  () -> {
                    try {
                      // Create a counter for periodic flushing within this chunk
                      int vectorsProcessedInChunk = 0;
                      
                      // Process each vector in the chunk
                      for (int i = 0; i < chunkLength; i++) {
                        // If we've processed a significant portion of the chunk and memory limit is a concern,
                        // we might need to flush what we have so far
                        if (memoryLimitMB > 0 && vectorsProcessedInChunk > 0 && vectorsProcessedInChunk % 100 == 0) {
                          // Check if memory usage is high
                          long currentMemory = totalBufferMemory.get();
                          long memoryLimitBytes = memoryLimitMB * 1024L * 1024L;
                          
                          if (currentMemory > memoryLimitBytes * 0.7) {
                            // We're approaching the memory limit, try to flush what we have
                            try {
                              progressLock.lock();
                              
                              // We'll only flush the portion we've processed so far
                              float[][] partialBuffer = new float[vectorsProcessedInChunk][];
                              int validCount = 0;
                              
                              // Copy valid vectors to a new buffer
                              for (int j = 0; j < vectorsProcessedInChunk; j++) {
                                if (vectorBuffer[j] != null) {
                                  partialBuffer[validCount++] = vectorBuffer[j];
                                  
                                  // Account for memory being released
                                  long vMemUsage = (vectorBuffer[j].length * 4L) + 16L;
                                  totalBufferMemory.addAndGet(-vMemUsage);
                                  
                                  // Clear the reference in the original buffer
                                  vectorBuffer[j] = null;
                                }
                              }
                              
                              // Only write if we have valid vectors
                              if (validCount > 0) {
                                // Resize the buffer if needed
                                float[][] validBuffer = validCount < partialBuffer.length ? 
                                    Arrays.copyOf(partialBuffer, validCount) : partialBuffer;
                                
                                // Write to disk
                                finalFvecWriter.writeBulk(validBuffer);
                                finalFvecWriter.flush();
                                
                                logger.debug("Flushed {} vectors during chunk processing to manage memory", validCount);
                                vectorsProcessedInChunk = 0; // Reset counter
                              }
                            } finally {
                              progressLock.unlock();
                            }
                          }
                        }
                        
                        vectorsProcessedInChunk++;
                        long currentIndex = cs + i;

                        // Get the index from ivec file
                        int[] indexVector = ivecReaderFinal.get((int) currentIndex);
                        int fvecIndex = indexVector[0];

                        // Validate index is within bounds for fvec file
                        if (fvecIndex < 0 || fvecIndex >= fvecReaderFinal.getSize()) {
                          throw new IllegalArgumentException(
                              "Index " + fvecIndex + " at position " + currentIndex
                              + " is out of bounds for fvec file with size "
                              + fvecReaderFinal.getSize());
                        }

                        indexBuffer[i] = fvecIndex;

                        // Get the vector from fvec file and track memory
                        float[] vector = fvecReaderFinal.get(fvecIndex);

                        // Calculate actual memory used by this vector (4 bytes per float plus overhead)
                        long vectorMemoryUsage =
                            (vector.length * 4L) + 16L; // 16 bytes for object overhead

                        // Add to memory tracking
                        long newTotal = totalBufferMemory.addAndGet(vectorMemoryUsage);

                        // Check if we've exceeded memory limit
                        if (memoryLimitMB > 0) {
                          long memoryLimitBytes = memoryLimitMB * 1024L * 1024L;
                          if (newTotal > memoryLimitBytes * 0.8) { // Start flushing at 80% of limit
                            // CRITICAL SECTION - Need to start flushing buffers to disk
                            try {
                              progressLock.lock();
                              
                              // Check again in case another thread flushed while we were waiting
                              if (totalBufferMemory.get() > memoryLimitBytes * 0.8) {
                                logger.warn(
                                    "Buffer memory usage ({} MB) exceeds 80% of memory limit ({}MB), flushing to disk",
                                    newTotal / (1024.0 * 1024.0),
                                    memoryLimitMB
                                );
                                
                                // Flush this chunk early to reduce memory pressure
                                // Make a copy to avoid concurrent modification issues
                                float[][] bufferCopy = Arrays.copyOf(vectorBuffer, i+1);
                                int validVectors = 0;
                                
                                // Count valid vectors
                                for (int vi = 0; vi <= i; vi++) {
                                  if (bufferCopy[vi] != null) {
                                    validVectors++;
                                  }
                                }
                                
                                // Only flush if we have valid vectors
                                if (validVectors > 0) {
                                  // Create an array of just the valid vectors
                                  float[][] validBufferCopy = new float[validVectors][];
                                  int validIdx = 0;
                                  
                                  // Copy valid vectors
                                  for (int vi = 0; vi <= i; vi++) {
                                    if (bufferCopy[vi] != null) {
                                      validBufferCopy[validIdx++] = bufferCopy[vi];
                                      
                                      // Clear memory in original buffer and account for it
                                      long vMemUsage = (bufferCopy[vi].length * 4L) + 16L;
                                      totalBufferMemory.addAndGet(-vMemUsage);
                                      
                                      // Clear the original reference
                                      vectorBuffer[vi] = null;
                                    }
                                  }
                                  
                                  // Write the vectors to disk
                                  try {
                                    finalFvecWriter.writeBulk(validBufferCopy);
                                    finalFvecWriter.flush();
                                    logger.info("Flushed {} vectors to disk to reduce memory pressure", validVectors);
                                    
                                    // Update counters - these vectors are done
                                    // We're still processing chunk i so we don't update processedVectors
                                  } catch (Exception e) {
                                    logger.error("Error flushing buffer: {}", e.getMessage(), e);
                                  }
                                }
                              }
                            } finally {
                              progressLock.unlock();
                              
                              // Suggest garbage collection after a flush
                              System.gc();
                            }
                          }
                        }

                        // Store in buffer
                        vectorBuffer[i] = vector;

                        // Update progress counter only
                        processedVectorsFinal.incrementAndGet();

                        // Progress display is now handled by the dedicated thread
                        // So we don't try to update the display from worker threads
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(
                          "Error processing chunk " + chunkIndex + ": " + e.getMessage(),
                          e
                      );
                    }
                  }, executor
              );

              futures.add(future);
            }

            // Create a final copy of the futures list before joining
            final List<CompletableFuture<Void>> finalFutures = new ArrayList<>(futures);

            // Wait for all chunks to complete
            CompletableFuture<Void> allFutures =
                CompletableFuture.allOf(finalFutures.toArray(new CompletableFuture[0]));

            // Handle any exceptions
            try {
              allFutures.join();
            } catch (Exception e) {
              logger.error("Error during parallel extraction: {}", e.getMessage(), e);

              // Stop progress thread if it's running
              if (progressThread != null) {
                progressThread.interrupt();
              }

              return 1;
            }

            // Stop progress thread now that processing is complete
            if (progressThread != null) {
              progressThread.interrupt();
              try {
                progressThread.join(1000); // Wait up to 1 second for it to finish
              } catch (InterruptedException ignored) {
                // Ignore interruption during cleanup
              }
            }
            
            // Stop memory monitor thread if running
            if (memoryMonitorThread != null) {
              memoryMonitorThread.interrupt();
              try {
                memoryMonitorThread.join(1000); // Wait up to 1 second for it to finish
              } catch (InterruptedException ignored) {
                // Ignore interruption during cleanup
              }
            }

            // Write all vectors to the output in the correct order
            logger.info("All chunks processed, writing vectors to output file...");

            // Make a final copy of the chunk buffers for writing
            final List<float[][]> finalChunkBuffers = new ArrayList<>(chunkBuffers);
            final FvecVectorWriter fvecWriterFinal = fvecWriter;

            // Ensure writer is properly flushed after all writing
            try {
                if (fvecWriterFinal != null) {
                    fvecWriterFinal.flush(); // Ensure any buffered data is written
                }
            } catch (Exception e) {
                logger.warn("Error flushing writer: {}", e.getMessage());
            }
            
            int totalWritten = 0;
            // Use bulk write instead of individual writes for better performance
            for (int c = 0; c < finalChunkBuffers.size(); c++) {
              float[][] vectors = finalChunkBuffers.get(c);
              if (vectors != null) {
                // Count valid vectors for bulk writing
                int validVectorCount = 0;
                for (float[] vector : vectors) {
                  if (vector != null) {
                    validVectorCount++;
                  }
                }
                
                if (validVectorCount > 0) {
                  // Create array of valid vectors for bulk writing
                  float[][] validVectors = new float[validVectorCount][];
                  int validIdx = 0;
                  for (float[] vector : vectors) {
                    if (vector != null) {
                      validVectors[validIdx++] = vector;
                    }
                  }
                  
                  try {
                    // Use bulk write for better performance and ensure it's written to disk
                    fvecWriterFinal.writeBulk(validVectors);
                    fvecWriterFinal.flush(); // Explicitly flush after each chunk
                    
                    // Update memory tracking
                    for (float[] vector : validVectors) {
                      // Track memory being released
                      long vectorMemoryUsage = (vector.length * 4L) + 16L; // 16 bytes for object overhead
                      totalBufferMemory.addAndGet(-vectorMemoryUsage);
                    }
                    
                    totalWritten += validVectorCount;
                    
                    logger.debug("Wrote chunk {} with {} vectors", c, validVectorCount);
                  } catch (Exception e) {
                    logger.error("Error writing chunk {}: {}", c, e.getMessage(), e);
                    throw e; // Re-throw to properly handle the error
                  }
                }
                
                // Clear the processed chunk to allow GC to reclaim memory
                finalChunkBuffers.set(c, null);
              }
              
              // Flush every few chunks
              if (c % 5 == 0) {
                try {
                  fvecWriterFinal.flush();
                  System.out.println("Progress: wrote " + totalWritten + " vectors so far...");
                  System.out.flush();
                } catch (Exception e) {
                  logger.warn("Error during flush: {}", e.getMessage());
                }
              }
            }

            // Ensure final flush of all data
            try {
                if (fvecWriterFinal != null) {
                    fvecWriterFinal.flush();
                }
            } catch (Exception e) {
                logger.warn("Error during final flush: {}", e.getMessage());
            }
            
            final int finalTotalWritten = totalWritten;
            logger.info("Wrote {} vectors to output file", finalTotalWritten);
            
            // Flush standard output to ensure visibility
            System.out.flush();

            // Clear buffer references to allow GC to reclaim memory
            for (int i = 0; i < chunkBuffers.size(); i++) {
              // Calculate array overhead to subtract from memory tracking
              float[][] vectorBuffer = chunkBuffers.get(i);
              int[] indexBuffer = chunkIndices.get(i);

              if (vectorBuffer != null) {
                // Subtract the buffer overhead that was added during creation
                long bufferOverhead = (vectorBuffer.length * 8L) + (
                    indexBuffer != null ? indexBuffer.length * 4L : 0);
                totalBufferMemory.addAndGet(-bufferOverhead);
              }

              chunkBuffers.set(i, null);
              chunkIndices.set(i, null);
            }

            // Final memory reporting
            logger.info(
                "Final buffer memory usage before cleanup: {} bytes",
                totalBufferMemory.get()
            );

            chunkBuffers.clear();
            chunkIndices.clear();
            futures.clear();

            // Reset memory counter to ensure accurate GC
            totalBufferMemory.set(0);

          } finally {
            // Shutdown the executor and wait for termination
            executor.shutdown();
            try {
              // Wait for tasks to complete or timeout after 30 seconds
              if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                // Force shutdown if tasks didn't complete in time
                executor.shutdownNow();
                logger.warn("Forced shutdown of executor service after timeout");
              }
            } catch (InterruptedException e) {
              // Re-interrupt the current thread
              Thread.currentThread().interrupt();
              // Force shutdown if interrupted
              executor.shutdownNow();
              logger.warn("Executor service shutdown interrupted");
            }
          }
        } else {
          // Single-threaded extraction for small datasets or when explicitly requested
          // Process in batches for better performance
          final int batchSize = 1000; // Process and write in batches
          float[][] batchVectors = new float[batchSize][];
          int batchCount = 0;
          
          for (long i = startIndex; i <= endIndex; i++) {
            try {
              // Get the index from ivec file
              int[] indexVector = ivecReader.get((int) i);
              int index = indexVector[0];
          
              // Check if the index is valid
              if (index < 0 || index >= fvecSize) {
                logger.error(
                    "Index {} from ivec file (at position {}) is out of bounds for fvec file (size: {})",
                    index,
                    i,
                    fvecSize
                );
                return 1;
              }
              
              // Get the vector from fvec file
              float[] vector = fvecReader.get(index);
              
              // Add to batch
              batchVectors[batchCount++] = vector;
              
              // Update progress counter
              processedVectors.incrementAndGet();
              
              // Write batch when full or at the end
              if (batchCount == batchSize || i == endIndex) {
                if (batchCount > 0) {
                  // If the batch isn't full, create a properly sized array
                  float[][] vectorsToWrite = batchCount < batchSize ? 
                      Arrays.copyOf(batchVectors, batchCount) : batchVectors;
                  
                  // Write batch and flush
                  fvecWriter.writeBulk(vectorsToWrite);
                  fvecWriter.flush();
                  
                  // Report progress periodically
                  if (i % 10000 < batchSize || i == endIndex) {
                    System.out.println("Progress: Processed " + processedVectors.get() + 
                        " of " + vectorCount + " vectors (" + 
                        String.format("%.1f%%", (100.0 * processedVectors.get() / vectorCount)) + ")");
                    System.out.flush();
                  }
                  
                  // Reset batch
                  batchCount = 0;
                  if (batchSize > 100) {
                    // Only perform GC periodically for large batches
                    batchVectors = new float[batchSize][]; // Create new array to let GC collect the old one
                  }
                }
              }
              
              // Periodically suggest garbage collection
              if (i % 50000 == 0 && i > 0) {
                System.gc();
              }

              // Progress reporting is now handled by the dedicated thread
            } catch (Exception e) {
              logger.error("Error processing vector at index {}: {}", i, e.getMessage());
              return 1;
            }
          }
        }

        // Stop the progress thread if it's running
        if (progressThread != null) {
          progressThread.interrupt();
          try {
            progressThread.join(1000); // Wait up to 1 second for it to finish
          } catch (InterruptedException ignored) {
            // Ignore interruption during cleanup
          }
        }

        // Display final progress
        if (displayActive.get()) {
          displayProgress(processedVectors.get(), vectorCount, startTime, 0);
        } else {
          displaySimpleProgress(processedVectors.get(), vectorCount, startTime, 0);
          System.out.println(); // Add a newline
        }

        // Add completion message
        if (displayActive.get()) {
          try {
            displayLines.add(new AttributedString(""));
            // Create an AttributedString directly for the completion message
            AttributedString completionMessage =
                new AttributedStringBuilder().style(AttributedStyle.BOLD.foreground(AttributedStyle.GREEN))
                    .append("Vector extraction completed successfully.").toAttributedString();
            displayLines.add(completionMessage);
          
            // Update the display - ensure the cursor is in a visible area
            display.resize(terminal.getHeight(), terminal.getWidth());
            display.update(displayLines, 0);
          
            terminal.writer().println();
            terminal.writer().println();
            terminal.flush();
            
            // Add a small delay to ensure the message is visible before shutdown
            try {
              Thread.sleep(500);
            } catch (InterruptedException ignored) {
              // Ignore
            }
            
            // Explicitly shutdown the display
            shutdownDisplay();
          } catch (Exception e) {
            logger.warn("Error updating final display: {}", e.getMessage());
            // Fallback to simple completion message
            System.out.println("Vector extraction completed successfully.");
            System.out.flush();
          }
        } else {
          System.out.println(); // Add newline after progress bar
          System.out.println("Vector extraction completed successfully.");
          System.out.flush(); // Ensure output is flushed to console
        }
        
        // Ensure memory counter is reset
        if (totalBufferMemory != null) {
          logger.debug("Resetting buffer memory counter from {} bytes", totalBufferMemory.get());
          totalBufferMemory.set(0);
        }
        
        // Explicitly flush all console output to ensure visibility
        System.out.flush();
        System.err.flush();

        // Clean up the terminal one more time to ensure it's properly closed
        cleanupTerminal();
        
        // Remove our shutdown hook since we're completing normally
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
          // Ignore - this happens if JVM is already shutting down
        }
        
        logger.info("Successfully wrote extracted vectors to {}", outputPath);
        return 0;

      } catch (Exception e) {
        // Handle invalid arguments or parsing errors
        if (e instanceof IllegalArgumentException) {
          logger.error("{}", e.getMessage());
          return 1;
        }
        // Other errors (I/O or unexpected)
        logger.error("Error during extraction: {}", e.getMessage(), e);
        return 2;
      } finally {
        // Close resources in finally block
        try {
          // Clean up readers and writer
          if (ivecReader != null) {
            try {
              ivecReader.close();
              logger.debug("Successfully closed ivecReader");
            } catch (Exception e) {
              logger.error("Error closing ivecReader: {}", e.getMessage(), e);
            } finally {
              ivecReader = null; // Ensure reference is cleared
            }
          }

          if (fvecReader != null) {
            try {
              fvecReader.close();
              logger.debug("Successfully closed fvecReader");
            } catch (Exception e) {
              logger.error("Error closing fvecReader: {}", e.getMessage(), e);
            } finally {
              fvecReader = null; // Ensure reference is cleared
            }
          }

          if (fvecWriter != null) {
            try {
              fvecWriter.close();
              logger.debug("Successfully closed fvecWriter");
            } catch (Exception e) {
              logger.error("Error closing fvecWriter: {}", e.getMessage(), e);
            } finally {
              fvecWriter = null; // Ensure reference is cleared
            }
          }


        } finally {
          // Clean up terminal resources
          if (terminalInitialized) {
            cleanupTerminal();
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error during extraction: {}", e.getMessage(), e);
      return 2;
    }
  }
}