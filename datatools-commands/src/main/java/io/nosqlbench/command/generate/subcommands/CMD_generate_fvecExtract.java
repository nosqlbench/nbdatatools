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


import io.nosqlbench.command.common.BatchProcessingOption;
import io.nosqlbench.command.common.OutputFileOption;
import io.nosqlbench.command.common.RangeOption;
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
 * Extract vectors from an fvec file using indices from an ivec file.
 *
 * This extraction process supports parallel processing with configurable thread count
 * and memory usage limits to balance performance with resource constraints.
 *
 * The extraction process includes the following safeguards and optimizations:
 * - Once the IvecReader is opened, the minimum and maximum range values are accessed to assert
 * a valid range. This ensures a fail-fast check occurs before a long process that might fail later.
 * - The extraction process can run concurrently using multiple threads for better performance.
 * - The user can specify the number of threads using the --threads option.
 * - An advisory limit on the amount of buffer memory can be set using the --memory-limit option.
 * - Range values are validated to ensure they are positive and within bounds of the ivec file.
 * - If any index value refers to a position that does not exist in the fvec file, an error is thrown. */
@CommandLine.Command(name = "fvec-extract",
    description = "Extract vectors from a fvec file using indices from an ivec file")
public class CMD_generate_fvecExtract implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(CMD_generate_fvecExtract.class);

  // JLine terminal display components
  private Terminal terminal;
  private Display display;
  private final List<AttributedString> displayLines = new ArrayList<>();
  private final AtomicBoolean displayActive = new AtomicBoolean(false);
  private Thread shutdownHook;

  @CommandLine.Mixin
  private OutputFileOption outputFileOption = new OutputFileOption();

  @CommandLine.Mixin
  private RangeOption rangeOption = new RangeOption();

  @CommandLine.Mixin
  private BatchProcessingOption batchProcessingOption = new BatchProcessingOption();

  @CommandLine.Option(names = {"--ivec-file"},
      description = "Path to ivec file containing indices",
      required = true)
  private String ivecFile;

  @CommandLine.Option(names = {"--fvec-file"},
      description = "Path to fvec file containing vectors to extract",
      required = true)
  private String fvecFile;

  @CommandLine.Option(names = {"--simple-progress"},
      description = "Use simple progress display instead of fancy terminal UI",
      defaultValue = "false")
  private boolean simpleProgress;

  /**
   * Calculate the memory requirement for a given vector dimension and count.
   *
   * @param dimension The dimension of each vector
   * @param count The number of vectors
   * @return The memory requirement in bytes
   */
  private long calculateMemoryRequirement(int dimension, long count) {
    // Each float is 4 bytes
    long bytesPerVector = dimension * 4L;
    long totalBytes = bytesPerVector * count;

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
   * @param processed The number of vectors processed
   * @param total The total number of vectors to process
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
      double vectorsPerSecond = elapsedSeconds > 0 ? (double) processed / elapsedSeconds : 0;

      // Estimate time remaining
      long remainingVectors = total - processed;
      long estimatedRemainingSeconds = vectorsPerSecond > 0 ? (long) (remainingVectors / vectorsPerSecond) : 0;

      // Format durations
      String elapsedStr = formatDuration(elapsed);
      String remainingStr = formatDuration(Duration.ofSeconds(estimatedRemainingSeconds));

      // Format memory usage
      String memoryUsageStr = formatByteSize(bufferMemory);
      String memoryLimitStr = batchProcessingOption.hasMemoryLimit() ? formatByteSize(batchProcessingOption.getMemoryLimitBytes()) : "unlimited";

      // Clear the display lines
      displayLines.clear();

      // Add header
      displayLines.add(new AttributedStringBuilder()
          .style(AttributedStyle.BOLD)
          .append("FVEC Extract Progress")
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
          .append("Input FVEC: ")
          .style(AttributedStyle.BOLD)
          .append(fvecFile)
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Output FVEC: ")
          .style(AttributedStyle.BOLD)
          .append(outputFileOption.getNormalizedOutputPath().toString())
          .style(AttributedStyle.DEFAULT)
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Range: ")
          .style(AttributedStyle.BOLD)
          .append(rangeOption.getRange() != null ? rangeOption.getRange().toString() : "all")
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
          .append(" vectors")
          .toAttributedString());

      displayLines.add(new AttributedStringBuilder()
          .append("Speed: ")
          .style(AttributedStyle.BOLD)
          .append(String.format("%,.1f", vectorsPerSecond))
          .style(AttributedStyle.DEFAULT)
          .append(" vectors/second")
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
   * @param processed The number of vectors processed
   * @param total The total number of vectors to process
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
    double vectorsPerSecond = elapsedSeconds > 0 ? (double) processed / elapsedSeconds : 0;

    // Estimate time remaining
    long remainingVectors = total - processed;
    long estimatedRemainingSeconds = vectorsPerSecond > 0 ? (long) (remainingVectors / vectorsPerSecond) : 0;

    // Format durations
    String elapsedStr = formatDuration(elapsed);
    String remainingStr = formatDuration(Duration.ofSeconds(estimatedRemainingSeconds));

    // Format memory usage
    String memoryUsageStr = formatByteSize(bufferMemory);
    String memoryLimitStr = batchProcessingOption.hasMemoryLimit() ? formatByteSize(batchProcessingOption.getMemoryLimitBytes()) : "unlimited";

    // Build a simple progress message
    StringBuilder message = new StringBuilder();
    message.append(String.format("Progress: %.1f%% (%,d/%,d)", progressPercent, processed, total));
    message.append(String.format(" | Speed: %,.1f vectors/sec", vectorsPerSecond));
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
    // Range is automatically parsed by picocli - just validate if needed
    if (rangeOption.getRange() != null) {
      try {
        // Validation happens in the Range record's compact constructor
        rangeOption.getRange();
      } catch (IllegalArgumentException e) {
        System.err.println("Error with range: " + e.getMessage());
        return 1;
      }
    }

    long startIndex = rangeOption.getRangeStart();
    long endIndex = rangeOption.getRangeEnd();
    long totalVectors = rangeOption.getRangeSize();

    // Check if output file exists
    Path outputPath = outputFileOption.getNormalizedOutputPath();
    if (outputFileOption.outputExistsWithoutForce()) {
      System.err.println("Error: Output file already exists. Use --force to overwrite.");
      return 1;
    }

    // Determine thread count
    int threadCount = batchProcessingOption.getEffectiveThreadCount();
    logger.info("Using {} threads based on available processors", threadCount);

    // Convert memory limit from MB to bytes
    long memoryLimitBytes = batchProcessingOption.getMemoryLimitBytes();

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

      // Open the fvec file to read vectors
      Path fvecPath = Paths.get(fvecFile);
      if (!Files.exists(fvecPath)) {
        System.err.println("Error: FVEC file does not exist: " + fvecFile);
        return 1;
      }

      // Create parent directories for output file if they don't exist
      Path outputParent = outputPath.getParent();
      if (outputParent != null) {
        Files.createDirectories(outputParent);
      }

      // Open the ivec file
      try (VectorFileArray<int[]> ivecReader = VectorFileIO.randomAccess(FileType.xvec, int[].class, ivecPath)) {
        // Validate range against ivec file size (endIndex is exclusive)
        long ivecSize = ivecReader.size();
        if (startIndex >= ivecSize || endIndex > ivecSize) {
          System.err.println("Warning: Range exceeds IVEC file size. File contains " + ivecSize + " indices. Truncating range.");
          // Truncate the range to the available indices
          if (startIndex >= ivecSize) {
            // If start is beyond the file size, there's nothing to extract
            System.err.println("Error: Start index " + startIndex + " is beyond the file size " + ivecSize);
            return 1;
          }
          // Adjust end index to be within bounds (endIndex is exclusive, so max is ivecSize)
          endIndex = Math.min(endIndex, ivecSize);
          totalVectors = endIndex - startIndex;
          logger.info("Adjusted range to [{}..{}) ({} vectors)", startIndex, endIndex, totalVectors);
        }

        // Get the dimension of the ivec file (should be 1 for indices)
        int ivecDimension = ivecReader.get(0).length;
        if (ivecDimension != 1) {
          System.err.println("Warning: IVEC file has dimension " + ivecDimension + ", expected 1.");
        }

        // Open the fvec file
        try (VectorFileArray<float[]> fvecReader = VectorFileIO.randomAccess(FileType.xvec, float[].class, fvecPath)) {
          // Get the dimension of the fvec file
          int fvecDimension = fvecReader.get(0).length;
          logger.info("FVEC dimension: {}", fvecDimension);

          // Get the size of the fvec file
          long fvecSize = fvecReader.size();
          logger.info("FVEC size: {}", fvecSize);

          // Create the output file
          try (VectorFileStreamStore<float[]> outputWriter = VectorFileIO.streamOut(FileType.xvec, float[].class, outputPath).orElseThrow(() ->
              new RuntimeException("Failed to create output file: " + outputPath))) {
            // Calculate memory requirement per batch
            int batchSize = batchProcessingOption.getBatchSize();
            long memoryPerBatch = calculateMemoryRequirement(fvecDimension, batchSize);
            logger.info("Estimated memory per batch: {}", formatByteSize(memoryPerBatch));

            // Adjust batch size if memory limit is specified
            if (memoryLimitBytes > 0 && memoryPerBatch * threadCount > memoryLimitBytes) {
              // Calculate new batch size to fit within memory limit
              long newBatchSize = (memoryLimitBytes / threadCount) / (fvecDimension * 4L);
              newBatchSize = Math.max(1, Math.min(newBatchSize, batchSize));
              logger.info("Adjusted batch size from {} to {} to fit within memory limit", batchSize, newBatchSize);
              batchSize = (int) newBatchSize;
            }

            // Process in batches
            long currentIndex = startIndex;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            AtomicLong currentMemoryUsage = new AtomicLong(0);
            Lock memoryLock = new ReentrantLock();

            while (currentIndex < endIndex) {
              // Determine batch end (exclusive, like endIndex)
              long batchEnd = Math.min(currentIndex + batchSize, endIndex);
              long batchVectorCount = batchEnd - currentIndex;

              // Reserve memory for this batch
              long batchMemory = calculateMemoryRequirement(fvecDimension, batchVectorCount);

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
                    displayProgress(processedCount.get(), totalVectors, startTime, currentMemoryUsage.get());
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
                  // Process each index in the batch (finalBatchEnd is exclusive)
                  for (long i = batchStart; i < finalBatchEnd; i++) {
                    // Read the index from the ivec file
                    int[] indexVector = ivecReader.get((int) i);
                    int index = indexVector[0];

                    // Validate the index
                    if (index < 0 || index >= fvecSize) {
                      throw new IllegalArgumentException(
                          "Invalid index " + index + " at position " + i + 
                          ". Must be between 0 and " + (fvecSize - 1));
                    }

                    // Read the vector from the fvec file
                    float[] vector = fvecReader.get(index);

                    // Write the vector to the output file
                    synchronized (outputWriter) {
                      outputWriter.write(vector);
                    }

                    // Update progress
                    processedCount.incrementAndGet();
                  }
                } catch (IllegalArgumentException e) {
                  // Rethrow the exception to be caught by the main thread
                  throw e;
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

              // Move to the next batch (batchEnd is exclusive, so it's the start of next batch)
              currentIndex = batchEnd;

              // Display progress periodically
              displayProgress(processedCount.get(), totalVectors, startTime, currentMemoryUsage.get());
            }

            // Wait for all futures to complete
            boolean allComplete = false;
            while (!allComplete) {
              // Update progress display
              displayProgress(processedCount.get(), totalVectors, startTime, currentMemoryUsage.get());

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
            displayProgress(processedCount.get(), totalVectors, startTime, 0);

            // Calculate total time
            Duration totalTime = Duration.between(startTime, Instant.now());
            System.out.println("\nExtraction completed in " + formatDuration(totalTime));
            System.out.println("Extracted " + processedCount.get() + " vectors to " + outputFileOption.getNormalizedOutputPath());

            return 0;
          }
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
    int exitCode = new CommandLine(new CMD_generate_fvecExtract())
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    System.exit(exitCode);
  }
}
