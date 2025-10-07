package io.nosqlbench.command.convert;

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


import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// Handles the conversion of a single vector file.
/// Used for parallel processing of multiple files.
public class SingleFileConverter implements Runnable {
  private static final Logger logger = LogManager.getLogger(SingleFileConverter.class);

  private final Path inputPath;
  private final Path outputPath;
  private final FileType inputFormat;
  private final FileType outputFormat;
  private final boolean normalize;
  private final int offset;
  private final Integer limit;
  private final int threads;
  private final boolean verbose;
  private final boolean quiet;
  private final CMD_convert.ConversionCallback callback;

  /// Creates a new single file converter.
  /// @param inputPath the path to the input vector file
  /// @param outputPath the path where the converted file will be written
  /// @param inputFormat the format of the input file
  /// @param outputFormat the format to convert the file to
  /// @param normalize whether to normalize vectors to unit length
  /// @param offset number of vectors to skip from the beginning of the input file
  /// @param limit maximum number of vectors to process, or null for all
  /// @param threads number of processor threads to use for conversion
  /// @param verbose whether to output detailed progress information
  /// @param quiet whether to suppress all output except errors
  /// @param callback callback to report conversion progress and results
  public SingleFileConverter(
      Path inputPath,
      Path outputPath,
      FileType inputFormat,
      FileType outputFormat,
      boolean normalize,
      int offset,
      Integer limit,
      int threads,
      boolean verbose,
      boolean quiet,
      CMD_convert.ConversionCallback callback
  )
  {
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.normalize = normalize;
    this.offset = offset;
    this.limit = limit;
    this.threads = threads;
    this.verbose = verbose;
    this.quiet = quiet;
    this.callback = callback;
  }

  @Override
  public void run() {
    try {
      if (Files.exists(outputPath)) {
        callback.onError(
            "Output file already exists: " + outputPath + " (use --force to overwrite)");
        return;
      }

      // Create parent directories if needed
      Path parent = outputPath.getParent();
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent);
      }

      BoundedVectorFileStream<float[]> streamer =
          VectorFileIO.streamIn(inputFormat, float[].class, inputPath).orElseThrow();
      // Open the input file
      Iterator<float[]> iterator = streamer.iterator();

      if (!iterator.hasNext()) {
        callback.onError("Input file is empty: " + inputPath);
        return;
      }

      // Get the vector dimension
      float[] firstVector = iterator.next();
      int dimension = firstVector.length;

      // Reopen the streamer for the full read
      streamer =
          VectorFileIO.streamIn(inputFormat, float[].class, inputPath).orElseThrow();
      iterator = streamer.iterator();

      VectorFileStreamStore<float[]> writer =
          VectorFileIO.streamOut(outputFormat, float[].class, outputPath).orElseThrow();

      // Determine the number of threads to use
      int threadCount = (threads > 0) ? threads : Runtime.getRuntime().availableProcessors();

      if (verbose) {
        logger.info(
            "[{}] Using {} processor threads with dimension {}",
            inputPath.getFileName(),
            threadCount,
            dimension
        );
      }

      try {
        // Skip initial vectors if offset > 0
        for (int i = 0; i < offset && iterator.hasNext(); i++) {
          iterator.next();
        }

        // Create thread-safe queues
        BlockingQueue<float[]> processingQueue = new LinkedBlockingQueue<>(1000);
        BlockingQueue<float[]> writingQueue = new LinkedBlockingQueue<>(1000);

        // Flags and counters for coordination
        AtomicBoolean readerDone = new AtomicBoolean(false);
        AtomicBoolean processorError = new AtomicBoolean(false);
        AtomicInteger activeProcessors = new AtomicInteger(threadCount);
        AtomicInteger totalProcessed = new AtomicInteger(0);

        // Initialize count estimate based on limit or file size
        Integer estimatedTotal = limit;

        // If no limit is specified, try to make an initial estimate based on file size
        if (estimatedTotal == null) {
          try {
            long fileSize = Files.size(inputPath);

            // Very rough initial estimate based on dimension and file size
            if (fileSize > 0) {
              long bytesPerVector = 4L * dimension; // 4 bytes per float * dimension
              long initialEstimate = fileSize / bytesPerVector;

              // Cap the estimate at Integer.MAX_VALUE
              if (initialEstimate < Integer.MAX_VALUE) {
                estimatedTotal = (int) initialEstimate;
              }
            }
          } catch (IOException e) {
            logger.warn("Could not determine size of {}: {}", inputPath, e.getMessage());
          }
        }

        // Create progress tracker with initial estimate
        ConversionProgress progress = new ConversionProgress(estimatedTotal, 0);

        // Create thread pool
        ExecutorService processorPool = Executors.newFixedThreadPool(threadCount);

        // Start the reader thread
        Iterator<float[]> finalIterator = iterator;
        Thread readerThread = new Thread(() -> {
          try {
            int count = 0;
            while (finalIterator.hasNext() && (limit == null || count < limit)) {
              float[] vector = finalIterator.next();
              processingQueue.put(vector);
              count++;
            }
          } catch (Exception e) {
            logger.error(
                "[{}] Error in reader thread: {}",
                inputPath.getFileName(),
                e.getMessage()
            );
            processorError.set(true);
          } finally {
            readerDone.set(true);
          }
        });
        readerThread.start();

        // Start processor threads
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
              logger.error(
                  "[{}] Error in processor thread: {}",
                  inputPath.getFileName(),
                  e.getMessage()
              );
              processorError.set(true);
            } finally {
              activeProcessors.decrementAndGet();
            }
          });
        }

        // Start writer thread
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
          } catch (Exception e) {
            logger.error(
                "[{}] Error in writer thread: {}",
                inputPath.getFileName(),
                e.getMessage()
            );
            processorError.set(true);
          }
        });
        writerThread.start();

        // Wait for threads to complete
        readerThread.join();
        processorPool.shutdown();
        processorPool.awaitTermination(1, TimeUnit.HOURS);
        writerThread.join();

        if (processorError.get()) {
          callback.onError("Errors occurred during processing file: " + inputPath);
          return;
        }

        if (!quiet) {
          logger.info(
              "[{}] Converted successfully: {} vectors, {}",
              inputPath.getFileName(),
              progress.getProcessedCount(),
              inputFormat + " → " + outputFormat
          );
        }

        callback.onSuccess(inputPath, outputPath, progress.getProcessedCount());
      } finally {
        writer.close();
      }
    } catch (Exception e) {
      logger.error("[{}] Error during conversion: {}", inputPath.getFileName(), e.getMessage(), e);
      callback.onError("Failed to convert " + inputPath + ": " + e.getMessage());
    }
  }

  /// Normalize a vector to unit length (L2 normalization)
  /// @param vector the vector to normalize
  private void normalizeVector(float[] vector) {
    double sumSquares = 0.0;
    for (float v : vector) {
      sumSquares += v * v;
    }
    double magnitude = Math.sqrt(sumSquares);

    if (magnitude < 1e-10) {
      return;
    }

    for (int i = 0; i < vector.length; i++) {
      vector[i] = (float) (vector[i] / magnitude);
    }
  }
}
