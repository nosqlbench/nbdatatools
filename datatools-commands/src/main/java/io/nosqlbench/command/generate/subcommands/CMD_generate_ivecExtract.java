package io.nosqlbench.command.generate.subcommands;

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


import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
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
 * Extract indices from an ivec file using a range specification.
 *
 * This extraction process supports parallel processing with configurable thread count
 * and memory usage limits to balance performance with resource constraints.
 *
 * The extraction process includes the following safeguards and optimizations:
 * - It uses random access interfaces through VectorFileIO helper methods for reading input files.
 * - The extraction process can run concurrently using multiple threads for better performance.
 * - The user can specify the number of threads using the --threads option.
 * - An advisory limit on the amount of buffer memory can be set using the --memory-limit option.
 * - Range values are validated to ensure they are positive and within bounds of the ivec file.
 */
@CommandLine.Command(name = "ivec-extract",
    description = "Extract indices from an ivec file using a range specification")
public class CMD_generate_ivecExtract implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(CMD_generate_ivecExtract.class);

  // JLine terminal display components
  private Terminal terminal;
  private Display display;
  private final List<AttributedString> displayLines = new ArrayList<>();
  private final AtomicBoolean displayActive = new AtomicBoolean(false);
  private Thread shutdownHook;

  @CommandLine.Option(names = {"--ivec-file"},
      description = "Path to ivec file containing indices to extract from",
      required = true)
  private String ivecFile;

  @CommandLine.Option(names = {"--range"},
      description = "Range of indices to extract (format: start..end), e.g. 0..1000 or 2000..3000",
      required = true)
  private String range;

  @CommandLine.Option(names = {"--output"},
      description = "Path to output ivec file",
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

  @CommandLine.Option(names = {"--memory-limit"},
      description = "Maximum memory to use for buffering in MB (0 = no limit)",
      defaultValue = "1024")
  private long memoryLimitMB;

  @CommandLine.Option(names = {"--batch-size"},
      description = "Number of indices to process in each batch",
      defaultValue = "10000")
  private int batchSize;

  @CommandLine.Option(names = {"--simple-progress"},
      description = "Use simple progress display instead of fancy terminal UI",
      defaultValue = "false")
  private boolean simpleProgress;

  /**
   * Parse a range string in the format "start..end" into a long array with two elements.
   *
   * @param rangeStr The range string to parse
   * @return A long array with two elements: [start, end]
   */
  private long[] parseRange(String rangeStr) {
    if (rangeStr == null || rangeStr.isEmpty()) {
      throw new IllegalArgumentException("Range string cannot be empty");
    }

    String[] parts = rangeStr.split("\\.\\.");
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          "Range must be in format 'start..end', got: " + rangeStr);
    }

    try {
      long start = Long.parseLong(parts[0].trim());
      long end = Long.parseLong(parts[1].trim());

      if (start < 0 || end < 0) {
        throw new IllegalArgumentException("Range values must be non-negative");
      }

      if (start > end) {
        throw new IllegalArgumentException("Start value must be less than or equal to end value");
      }

      return new long[]{start, end};
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid range format: " + rangeStr, e);
    }
  }

  /**
   * Calculate the memory requirement for a given count of integers.
   *
   * @param count The number of integers
   * @return The memory requirement in bytes
   */
  private long calculateMemoryRequirement(long count) {
    // Each int is 4 bytes
    long bytesPerInt = 4L;
    long totalBytes = bytesPerInt * count;

    // Add some overhead for data structures
    long overhead = Math.max(1024 * 1024, totalBytes / 10); // At least 1MB or 10% of data size

    return totalBytes + overhead;
  }

  /**
   * Initialize the terminal for fancy progress display.
   *
   * @return true if terminal initialization was successful, false otherwise
   */
  private boolean initializeTerminal() {
    if (simpleProgress) {
      return false;
    }

    try {
      // Try to create a system terminal
      terminal = TerminalBuilder.builder()
          .system(true)
          .jna(true)
          .jansi(true)
          .build();

      // Create a display for the terminal
      display = new Display(terminal, true);

      // Register a shutdown hook to restore the terminal
      shutdownHook = new Thread(this::cleanupTerminal);
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      // Clear the screen and hide the cursor
      terminal.puts(InfoCmp.Capability.clear_screen);
      terminal.puts(InfoCmp.Capability.cursor_invisible);
      terminal.flush();

      displayActive.set(true);
      return true;
    } catch (IOException e) {
      logger.warn("Failed to initialize terminal for fancy progress display: {}", e.getMessage());
      logger.debug("Terminal initialization error", e);
      return false;
    }
  }

  /**
   * Shutdown the display and restore the terminal.
   */
  private void shutdownDisplay() {
    if (terminal != null && displayActive.get()) {
      try {
        // Show the cursor again
        terminal.puts(InfoCmp.Capability.cursor_visible);

        // Move to the bottom of the display
        int height = terminal.getHeight();
        if (height > 0) {
          terminal.puts(InfoCmp.Capability.cursor_address, height - 1, 0);
        }

        // Print a newline to ensure the prompt appears below our output
        terminal.writer().println();
        terminal.writer().println();
        terminal.flush();

        // Remove the shutdown hook since we're manually cleaning up
        try {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException e) {
          // Ignore if VM is already shutting down
        }

        displayActive.set(false);
      } catch (Exception e) {
        logger.warn("Error shutting down display: {}", e.getMessage());
        logger.debug("Display shutdown error", e);
      }
    }
  }

  /**
   * Clean up the terminal resources.
   */
  private void cleanupTerminal() {
    if (terminal != null) {
      try {
        // Show the cursor again
        terminal.puts(InfoCmp.Capability.cursor_visible);
        terminal.flush();

        // Close the terminal
        terminal.close();
      } catch (Exception e) {
        // Just log at debug level since this is a shutdown hook
        logger.debug("Error cleaning up terminal: {}", e.getMessage());
      } finally {
        terminal = null;
        display = null;
        displayActive.set(false);
      }
    }
  }

  /**
   * Display progress information in the terminal.
   *
   * @param processed The number of indices processed
   * @param total The total number of indices to process
   * @param startTime The start time of the operation
   * @param bufferMemory The current buffer memory usage in bytes
   */
  private void displayProgress(long processed, long total, Instant startTime, long bufferMemory) {
    if (simpleProgress || terminal == null || !displayActive.get()) {
      displaySimpleProgress(processed, total, startTime, bufferMemory);
      return;
    }

    try {
      // Calculate progress percentage
      double progressPercent = (double) processed / total * 100.0;

      // Calculate elapsed time and estimated time remaining
      Duration elapsed = Duration.between(startTime, Instant.now());
      long elapsedSeconds = elapsed.getSeconds();

      // Avoid division by zero
      double indicesPerSecond = elapsedSeconds > 0 ? (double) processed / elapsedSeconds : 0;

      // Estimate time remaining
      long remainingIndices = total - processed;
      long estimatedRemainingSeconds = indicesPerSecond > 0 ? (long) (remainingIndices / indicesPerSecond) : 0;

      // Format durations
      String elapsedStr = formatDuration(elapsed);
      String remainingStr = formatDuration(Duration.ofSeconds(estimatedRemainingSeconds));

      // Format memory usage
      String memoryUsageStr = formatByteSize(bufferMemory);
      String memoryLimitStr = memoryLimitMB > 0 ? formatByteSize(memoryLimitMB * 1024 * 1024) : "unlimited";

      // Clear the display lines
      displayLines.clear();

      // Add header
      displayLines.add(new AttributedStringBuilder()
          .style(AttributedStyle.BOLD)
          .append("IVEC Extract Progress")
          .toAttributedString());

      displayLines.add(new AttributedString(""));

      // Add file information
      displayLines.add(new AttributedStringBuilder()
          .append("Input IVEC: ")
          .style(AttributedStyle.BOLD)
          .append(ivecFile)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Output IVEC: ")
          .style(AttributedStyle.BOLD)
          .append(outputFile)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Range: ")
          .style(AttributedStyle.BOLD)
          .append(range)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedString(""));

      // Add progress bar
      int width = Math.min(terminal.getWidth() - 10, 60);
      int completed = (int) (width * progressPercent / 100);

      AttributedStringBuilder progressBar = new AttributedStringBuilder()
          .append("Progress: [")
          .style(AttributedStyle.BOLD.foreground(AttributedStyle.GREEN))
          .append("=".repeat(Math.max(0, completed)))
          .style(AttributedStyle.BOLD)
          .append(" ".repeat(Math.max(0, width - completed)))
          .style(AttributedStyle.DEFAULT)
          .append("] ")
          .style(AttributedStyle.BOLD)
          .append(String.format("%.1f%%", progressPercent))
          .style(AttributedStyle.DEFAULT);

      displayLines.add(progressBar.toAttributedString());

      // Add statistics
      displayLines.add(new AttributedStringBuilder()
          .append("Processed: ")
          .style(AttributedStyle.BOLD)
          .append(String.format("%,d", processed))
          .style(AttributedStyle.DEFAULT)
          .append(" of ")
          .style(AttributedStyle.BOLD)
          .append(String.format("%,d", total))
          .style(AttributedStyle.DEFAULT)
          .append(" indices")
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Speed: ")
          .style(AttributedStyle.BOLD)
          .append(String.format("%,.1f", indicesPerSecond))
          .style(AttributedStyle.DEFAULT)
          .append(" indices/second")
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Memory: ")
          .style(AttributedStyle.BOLD)
          .append(memoryUsageStr)
          .style(AttributedStyle.DEFAULT)
          .append(" / ")
          .style(AttributedStyle.BOLD)
          .append(memoryLimitStr)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedString(""));

      // Add time information
      displayLines.add(new AttributedStringBuilder()
          .append("Elapsed time: ")
          .style(AttributedStyle.BOLD)
          .append(elapsedStr)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Estimated time remaining: ")
          .style(AttributedStyle.BOLD)
          .append(remainingStr)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      // Update the display
      display.update(displayLines, terminal.getWidth());
    } catch (Exception e) {
      // If there's an error with the fancy display, fall back to simple progress
      logger.warn("Error updating progress display: {}", e.getMessage());
      logger.debug("Progress display error", e);
      displaySimpleProgress(processed, total, startTime, bufferMemory);
    }
  }

  /**
   * Display simple progress information on the console.
   *
   * @param processed The number of indices processed
   * @param total The total number of indices to process
   * @param startTime The start time of the operation
   * @param bufferMemory The current buffer memory usage in bytes
   */
  private void displaySimpleProgress(long processed, long total, Instant startTime, long bufferMemory) {
    // Calculate progress percentage
    double progressPercent = (double) processed / total * 100.0;

    // Calculate elapsed time and estimated time remaining
    Duration elapsed = Duration.between(startTime, Instant.now());
    long elapsedSeconds = elapsed.getSeconds();

    // Avoid division by zero
    double indicesPerSecond = elapsedSeconds > 0 ? (double) processed / elapsedSeconds : 0;

    // Estimate time remaining
    long remainingIndices = total - processed;
    long estimatedRemainingSeconds = indicesPerSecond > 0 ? (long) (remainingIndices / indicesPerSecond) : 0;

    // Format durations
    String elapsedStr = formatDuration(elapsed);
    String remainingStr = formatDuration(Duration.ofSeconds(estimatedRemainingSeconds));

    // Format memory usage
    String memoryUsageStr = formatByteSize(bufferMemory);
    String memoryLimitStr = memoryLimitMB > 0 ? formatByteSize(memoryLimitMB * 1024 * 1024) : "unlimited";

    // Build a simple progress message
    StringBuilder message = new StringBuilder();
    message.append(String.format("Progress: %.1f%% (%,d/%,d)", progressPercent, processed, total));
    message.append(String.format(" | Speed: %,.1f indices/sec", indicesPerSecond));
    message.append(String.format(" | Memory: %s/%s", memoryUsageStr, memoryLimitStr));
    message.append(String.format(" | Elapsed: %s", elapsedStr));
    message.append(String.format(" | Remaining: %s", remainingStr));

    // Print the message with a carriage return to overwrite the previous line
    System.out.print("\r" + message);

    // If we're done, add a newline
    if (processed >= total) {
      System.out.println();
    }
  }

  /**
   * Format a duration into a human-readable string.
   *
   * @param duration The duration to format
   * @return A formatted string representation of the duration
   */
  private String formatDuration(Duration duration) {
    long hours = duration.toHours();
    long minutes = duration.toMinutesPart();
    long seconds = duration.toSecondsPart();

    if (hours > 0) {
      return String.format("%dh %02dm %02ds", hours, minutes, seconds);
    } else if (minutes > 0) {
      return String.format("%dm %02ds", minutes, seconds);
    } else {
      return String.format("%ds", seconds);
    }
  }

  /**
   * Format a byte size into a human-readable string.
   *
   * @param bytes The size in bytes
   * @return A formatted string representation of the size
   */
  private String formatByteSize(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    } else {
      return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
  }

  /**
   * Main method to execute the command.
   *
   * @return The exit code (0 for success, non-zero for failure)
   */
  @Override
  public Integer call() {
    // Parse the range
    long[] rangeValues;
    try {
      rangeValues = parseRange(range);
    } catch (IllegalArgumentException e) {
      System.err.println("Error parsing range: " + e.getMessage());
      return 1;
    }

    long startIndex = rangeValues[0];
    long endIndex = rangeValues[1];
    long totalIndices = endIndex - startIndex + 1;

    // Check if output file exists
    Path outputPath = Paths.get(outputFile);
    if (Files.exists(outputPath) && !force) {
      System.err.println("Error: Output file already exists. Use --force to overwrite.");
      return 1;
    }

    // Determine thread count
    int threadCount = threads;
    if (threadCount <= 0) {
      threadCount = Runtime.getRuntime().availableProcessors();
      logger.info("Using {} threads based on available processors", threadCount);
    }

    // Convert memory limit from MB to bytes
    long memoryLimitBytes = memoryLimitMB * 1024 * 1024;

    // Initialize terminal for progress display
    boolean fancyDisplay = initializeTerminal();
    logger.info("Using {} progress display", fancyDisplay ? "fancy" : "simple");

    // Create executor service for parallel processing
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    // Track progress
    AtomicLong processedCount = new AtomicLong(0);
    Instant startTime = Instant.now();

    try {
      // Open the ivec file to read indices
      Path ivecPath = Paths.get(ivecFile);
      if (!Files.exists(ivecPath)) {
        System.err.println("Error: IVEC file does not exist: " + ivecFile);
        return 1;
      }

      // Create parent directories for output file if they don't exist
      Path outputParent = outputPath.getParent();
      if (outputParent != null) {
        Files.createDirectories(outputParent);
      }

      // Open the ivec file using VectorFileArray for random access
      try (VectorFileArray<int[]> ivecReader = VectorFileIO.randomAccess(FileType.xvec, int[].class, ivecPath)) {
        // Validate range against ivec file size
        long ivecSize = ivecReader.size();
        if (startIndex >= ivecSize || endIndex >= ivecSize) {
          System.err.println("Warning: Range exceeds IVEC file size. File contains " + ivecSize + " indices. Truncating range.");
          // Truncate the range to the available indices
          if (startIndex >= ivecSize) {
            // If start is beyond the file size, there's nothing to extract
            System.err.println("Error: Start index " + startIndex + " is beyond the file size " + ivecSize);
            return 1;
          }
          // Adjust end index to be within bounds
          endIndex = Math.min(endIndex, ivecSize - 1);
          totalIndices = endIndex - startIndex + 1;
          logger.info("Adjusted range to {}..{} ({} indices)", startIndex, endIndex, totalIndices);
        }

        // Get the dimension of the ivec file (should be 1 for indices)
        int ivecDimension = ivecReader.get(0).length;
        if (ivecDimension != 1) {
          System.err.println("Warning: IVEC file has dimension " + ivecDimension + ", expected 1.");
        }

        // Create the output file
        try (VectorFileStreamStore<int[]> outputWriter = VectorFileIO.streamOut(FileType.xvec, int[].class, outputPath).orElseThrow(() ->
            new RuntimeException("Failed to create output file: " + outputPath))) {
          // Calculate memory requirement per batch
          long memoryPerBatch = calculateMemoryRequirement(batchSize);
          logger.info("Estimated memory per batch: {}", formatByteSize(memoryPerBatch));

          // Adjust batch size if memory limit is specified
          if (memoryLimitBytes > 0 && memoryPerBatch * threadCount > memoryLimitBytes) {
            // Calculate new batch size to fit within memory limit
            long newBatchSize = (memoryLimitBytes / threadCount) / 4L;
            newBatchSize = Math.max(1, Math.min(newBatchSize, batchSize));
            logger.info("Adjusted batch size from {} to {} to fit within memory limit", batchSize, newBatchSize);
            batchSize = (int) newBatchSize;
          }

          // Process in batches
          long currentIndex = startIndex;
          List<CompletableFuture<Void>> futures = new ArrayList<>();
          AtomicLong currentMemoryUsage = new AtomicLong(0);
          Lock memoryLock = new ReentrantLock();

          while (currentIndex <= endIndex) {
            // Determine batch end
            long batchEnd = Math.min(currentIndex + batchSize - 1, endIndex);
            long batchSize = batchEnd - currentIndex + 1;

            // Reserve memory for this batch
            long batchMemory = calculateMemoryRequirement(batchSize);

            // If memory limit is specified, wait until there's enough memory available
            if (memoryLimitBytes > 0) {
              boolean memoryAvailable = false;
              while (!memoryAvailable) {
                memoryLock.lock();
                try {
                  long currentUsage = currentMemoryUsage.get();
                  if (currentUsage + batchMemory <= memoryLimitBytes) {
                    currentMemoryUsage.addAndGet(batchMemory);
                    memoryAvailable = true;
                  }
                } finally {
                  memoryLock.unlock();
                }

                if (!memoryAvailable) {
                  // Wait a bit before checking again
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for memory", e);
                  }

                  // Update progress display while waiting
                  displayProgress(processedCount.get(), totalIndices, startTime, currentMemoryUsage.get());
                }
              }
            }

            // Create a final copy of the current index and batch end for the lambda
            final long batchStart = currentIndex;
            final long finalBatchEnd = batchEnd;
            final long batchMemoryFinal = batchMemory;

            // Submit batch processing task
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
              try {
                // Process each index in the batch
                for (long i = batchStart; i <= finalBatchEnd; i++) {
                  // Read the index from the ivec file using random access
                  int[] indexVector = ivecReader.get((int) i);

                  // Write the index to the output file
                  synchronized (outputWriter) {
                    outputWriter.write(indexVector);
                  }

                  // Update progress
                  processedCount.incrementAndGet();
                }
              } finally {
                // Release memory
                if (memoryLimitBytes > 0) {
                  memoryLock.lock();
                  try {
                    currentMemoryUsage.addAndGet(-batchMemoryFinal);
                  } finally {
                    memoryLock.unlock();
                  }
                }
              }
            }, executor);

            futures.add(future);

            // Move to the next batch
            currentIndex = batchEnd + 1;

            // Display progress periodically
            displayProgress(processedCount.get(), totalIndices, startTime, currentMemoryUsage.get());
          }

          // Wait for all futures to complete
          boolean allComplete = false;
          while (!allComplete) {
            // Update progress display
            displayProgress(processedCount.get(), totalIndices, startTime, currentMemoryUsage.get());

            // Check if all futures are complete
            allComplete = true;
            for (CompletableFuture<Void> future : futures) {
              if (!future.isDone()) {
                allComplete = false;
                break;
              }
            }

            if (!allComplete) {
              // Wait a bit before checking again
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while waiting for tasks to complete");
                break;
              }
            }
          }

          // Check for exceptions in futures
          boolean hasErrors = false;
          for (CompletableFuture<Void> future : futures) {
            try {
              future.join();
            } catch (Exception e) {
              Throwable cause = e.getCause();
              System.err.println("Error processing batch: " + (cause != null ? cause.getMessage() : e.getMessage()));
              hasErrors = true;
            }
          }

          if (hasErrors) {
            // Delete the output file if it was created
            try {
              Files.deleteIfExists(outputPath);
            } catch (IOException e) {
              logger.warn("Failed to delete output file after error: {}", e.getMessage());
            }
            return 1;
          }

          // Final progress update
          displayProgress(processedCount.get(), totalIndices, startTime, 0);

          // Calculate total time
          Duration totalTime = Duration.between(startTime, Instant.now());
          System.out.println("\nExtraction completed in " + formatDuration(totalTime));
          System.out.println("Extracted " + processedCount.get() + " indices to " + outputFile);

          return 0;
        }
      }
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
      return 1;
    } finally {
      // Shutdown the executor
      executor.shutdown();
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      // Shutdown the display
      shutdownDisplay();
    }
  }

  /**
   * Main method for standalone execution.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    int exitCode = new CommandLine(new CMD_generate_ivecExtract())
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    System.exit(exitCode);
  }
}