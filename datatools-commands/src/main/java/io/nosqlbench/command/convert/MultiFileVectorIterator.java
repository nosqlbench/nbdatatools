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


import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.nbvectors.datasource.parquet.conversion.ConverterType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 An iterator that seamlessly combines vectors from multiple input files.
 Handles dimension consistency checks and provides a unified view of vectors
 from all input files. */
public class MultiFileVectorIterator implements Iterator<float[]> {
  private static final Logger logger = LogManager.getLogger(MultiFileVectorIterator.class);

  private final List<Path> inputPaths;
  private final FileType filetype;
  private final Class<?> datatype;
  private int currentFileIndex = 0;
  private Iterator<float[]> currentIterator = null;
  private BoundedVectorFileStream currentStreamer = null;
  private final int dimension;
  private final boolean verbose;
  private long totalVectorsRead = 0;
  private final String converterType; // Added for parquet converter selection

  /**
   Creates a new iterator over multiple vector files.
   @param inputPaths
   List of file paths to process
   @param dimension
   Expected vector dimension
   @param verbose
   Whether to log verbose information
   @throws Exception
   If there's an error opening files or inconsistent dimensions
   */
  public MultiFileVectorIterator(
      List<Path> inputPaths,
      int dimension,
      boolean verbose,
      FileType filetype,
      Class<?> dataType
  ) throws Exception
  {
    this(inputPaths, dimension, verbose, filetype, dataType, "hfembed");
  }

  /**
   * Creates a new iterator over multiple vector files with a specific converter type for parquet files.
   * @param inputPaths List of file paths to process
   * @param dimension Expected vector dimension
   * @param verbose Whether to log verbose information
   * @param filetype The file type of the input files
   * @param dataType The data type of the vectors
   * @param converterType The converter type to use for parquet files
   * @throws Exception If there's an error opening files or inconsistent dimensions
   */
  public MultiFileVectorIterator(
      List<Path> inputPaths,
      int dimension,
      boolean verbose,
      FileType filetype,
      Class<?> dataType,
      String converterType
  ) throws Exception
  {
    this.inputPaths = inputPaths;
    this.dimension = dimension;
    this.verbose = verbose;
    this.filetype = filetype;
    this.datatype = dataType;
    this.converterType = converterType;

    if (inputPaths.isEmpty()) {
      throw new IllegalArgumentException("No input files provided");
    }

    // Initialize the first file
    moveToNextFile();
  }

  /**
   Advances to the next input file.
   @return true if successfully moved to next file, false if no more files
   @throws Exception
   if there's an error opening the file
   */
  private boolean moveToNextFile() throws Exception {
    if (currentStreamer != null) {
      try {
        currentStreamer.close();
      } catch (Exception e) {
        logger.warn("Error closing streamer: {}", e.getMessage());
      }
      currentStreamer = null;
      currentIterator = null;
    }

    if (currentFileIndex >= inputPaths.size()) {
      return false;
    }

    Path filePath = inputPaths.get(currentFileIndex);
    if (verbose) {
      logger.info(
          "Opening input file {} of {}: {}",
          currentFileIndex + 1,
          inputPaths.size(),
          filePath
      );
    }

    currentStreamer =
        VectorFileIO.streamIn(filetype, datatype, filePath).orElseThrow();

    // If this is a parquet file and we have a ParquetVectorStreamer, set the converter type
    if (filetype == FileType.parquet && currentStreamer.getClass().getSimpleName().equals("ParquetVectorStreamer")) {
      try {
        // Use reflection to call the open method with converter type
        currentStreamer.getClass().getMethod("open", Path.class, String.class)
            .invoke(currentStreamer, filePath, converterType);
      } catch (Exception e) {
        logger.warn("Could not set converter type for ParquetVectorStreamer: {}", e.getMessage());
      }
    }

    currentIterator = currentStreamer.iterator();

    // Verify dimension if this isn't the first file
    if (currentFileIndex > 0 && currentIterator.hasNext()) {
      float[] firstVector = currentIterator.next();
      if (firstVector.length != dimension) {
        throw new IllegalStateException(
            "Inconsistent vector dimensions: expected " + dimension + " but got "
            + firstVector.length + " in file " + filePath);
      }

      // Recreate the streamer since we consumed the first vector
      currentStreamer =
          VectorFileIO.streamIn(filetype, datatype, filePath).orElseThrow();

      // If this is a parquet file and we have a ParquetVectorStreamer, set the converter type
      if (filetype == FileType.parquet && currentStreamer.getClass().getSimpleName().equals("ParquetVectorStreamer")) {
        try {
          // Use reflection to call the open method with converter type
          currentStreamer.getClass().getMethod("open", Path.class, String.class)
              .invoke(currentStreamer, filePath, converterType);
        } catch (Exception e) {
          logger.warn("Could not set converter type for ParquetVectorStreamer: {}", e.getMessage());
        }
      }

      currentIterator = currentStreamer.iterator();
    }

    currentFileIndex++;
    return true;
  }

  @Override
  public boolean hasNext() {
    try {
      // Check if current iterator has vectors
      if (currentIterator != null && currentIterator.hasNext()) {
        return true;
      }

      // Try to move to the next file until we find one with vectors or run out of files
      while (moveToNextFile()) {
        if (currentIterator.hasNext()) {
          return true;
        }
      }

      return false;
    } catch (Exception e) {
      logger.error("Error checking for next vector: {}", e.getMessage(), e);
      return false;
    }
  }

  @Override
  public float[] next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more vectors available");
    }

    float[] vector = currentIterator.next();
    totalVectorsRead++;
    return vector;
  }

  /**
   Gets the total number of vectors read so far.
   @return The count of vectors read
   */
  public long getTotalVectorsRead() {
    return totalVectorsRead;
  }

  /**
   Gets the estimated total number of vectors across all files.
   This is more accurate than file size estimation, especially for compressed formats like parquet.
   @return The estimated total number of vectors, or -1 if estimation fails
   */
  public int getTotalVectorCount() {
    try {
      int totalCount = 0;
      // Save current state
      int savedFileIndex = currentFileIndex;
      BoundedVectorFileStream<?> savedStreamer = currentStreamer;
      Iterator<float[]> savedIterator = currentIterator;

      // Reset to beginning
      currentFileIndex = 0;

      // Go through all files and sum their sizes
      for (Path filePath : inputPaths) {
        BoundedVectorFileStream<?> streamer = null;
        try {
            streamer = VectorFileIO.streamIn(filetype, datatype, filePath).orElse(null);
            if (streamer != null) {
              // If this is a parquet file and we have a ParquetVectorStreamer, set the converter type
              if (filetype == FileType.parquet && streamer.getClass().getSimpleName().equals("ParquetVectorStreamer")) {
                try {
                  // Use reflection to call the open method with converter type
                  streamer.getClass().getMethod("open", Path.class, String.class)
                      .invoke(streamer, filePath, converterType);
                } catch (Exception e) {
                  logger.warn("Could not set converter type for ParquetVectorStreamer: {}", e.getMessage());
                }
              }

              try {
                int size = streamer.getSize();
                if (size > 0) {
                  totalCount += size;
                  if (verbose) {
                    logger.info("File {} contains {} vectors", filePath, size);
                  }
                } else if (size < 0) {
                  // Size is unknown (no prescan performed)
                  if (verbose) {
                    logger.info("File {} size is unknown (prescan skipped)", filePath);
                  }
                } else {
                  // Size is 0
                  if (verbose) {
                    logger.info("File {} is empty", filePath);
                  }
                }
              } catch (Exception e) {
                logger.warn("Could not determine size of {}: {}", filePath, e.getMessage());
              }
            }
        } finally {
            if (streamer != null) {
                try {
                    streamer.close();
                } catch (Exception e) {
                    logger.warn("Error closing streamer for {}: {}", filePath, e.getMessage());
                }
            }
        }
      }

      // Restore state
      currentFileIndex = savedFileIndex;
      currentStreamer = savedStreamer;
      currentIterator = savedIterator;

      return totalCount;
    } catch (Exception e) {
      logger.error("Error estimating total vector count: {}", e.getMessage(), e);
      return -1;
    }
  }

  /**
   Gets the name of the current source file.
   @return The name of the current file being processed
   */
  public String getCurrentSourceName() {
    return currentStreamer != null ? currentStreamer.getName() : "none";
  }

  /**
   Closes all open resources.
   */
  public void close() {
    if (currentStreamer != null) {
      try {
        currentStreamer.close();
      } catch (Exception e) {
        logger.warn("Error closing streamer: {}", e.getMessage());
      }
      currentStreamer = null;
      currentIterator = null;
    }
  }

}
