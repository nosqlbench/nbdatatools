package io.nosqlbench.readers;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Utility helpers shared by reader implementations in this package.
public final class ReaderUtils {

  private static final int MAX_REASONABLE_DIMENSION = 1_048_576;

  private ReaderUtils() {
    // Utility class
  }

  /// Compute the logical vector count for a fixed-width record file ensuring int range safety.
  /// @param filePath The path to the file being inspected (used for diagnostics)
  /// @param fileSize The total size of the file in bytes
  /// @param recordSize The size in bytes for a single record including metadata
  /// @param elementWidth The width in bytes of each vector element (used for endianness check, 0 to skip)
  /// @return The number of vectors contained in the file
  /// @throws IOException If the file layout is invalid or the vector count exceeds int range
  static int computeVectorCount(Path filePath, long fileSize, int recordSize, int elementWidth) throws IOException {
    if (recordSize <= 0) {
      throw new IOException("Record size must be positive for file: " + filePath);
    }

    long recordSizeLong = recordSize;
    if (fileSize % recordSizeLong != 0) {
      String errorMessage;

      // If element width is provided, check if endianness might be the issue
      if (elementWidth > 0) {
        try {
          EndianCheckResult endianCheck = checkXvecEndianness(filePath, elementWidth);
          if (endianCheck.isEndianMismatch()) {
            errorMessage = "File " + filePath + " appears to use big-endian byte order, but little-endian is expected. "
                         + "The file size " + fileSize + " is not a multiple of the expected record size " + recordSizeLong
                         + " when interpreted as little-endian. "
                         + "However, the file appears to be readable when interpreted as big-endian "
                         + "(dimension=" + endianCheck.getBigEndianDimension()
                         + ", vectors=" + endianCheck.getBigEndianVectorCount() + "). "
                         + "Consider converting the file to little-endian format.";
          } else if (!endianCheck.isLittleEndianValid() && !endianCheck.isBigEndianValid()) {
            errorMessage = "File size " + fileSize + " is not a multiple of record size "
                         + recordSizeLong + " for file: " + filePath;
            String reason = endianCheck.getLittleEndianFailureReason();
            if (reason != null) {
              errorMessage += ". Additionally, dimension header validation failed: " + reason;
            }
          } else {
            errorMessage = "File size " + fileSize + " is not a multiple of record size "
                         + recordSizeLong + " for file: " + filePath;
          }
        } catch (IOException e) {
          // If endianness check fails, just use the original error message
          errorMessage = "File size " + fileSize + " is not a multiple of record size "
                       + recordSizeLong + " for file: " + filePath;
        }
      } else {
        errorMessage = "File size " + fileSize + " is not a multiple of record size "
                     + recordSizeLong + " for file: " + filePath;
      }

      throw new IOException(errorMessage);
    }

    long vectorCount = fileSize / recordSizeLong;
    if (vectorCount > Integer.MAX_VALUE) {
      throw new IOException("Vector count " + vectorCount + " exceeds maximum supported size "
                            + Integer.MAX_VALUE + " for file: " + filePath);
    }

    return (int) vectorCount;
  }

  /// Evaluate the dimension header of an xvec-style file under both endian assumptions.
  /// @param filePath The vector file to inspect
  /// @param elementWidth The width in bytes of each vector element
  /// @return An {@link EndianCheckResult} detailing which interpretations were valid
  /// @throws IOException If the file cannot be read or neither interpretation is valid
  public static EndianCheckResult checkXvecEndianness(Path filePath, int elementWidth) throws IOException {
    if (elementWidth <= 0) {
      throw new IOException("Element width must be positive for file: " + filePath);
    }

    EndianValidation little = evaluateXvecLayout(filePath, elementWidth, ByteOrder.LITTLE_ENDIAN);
    EndianValidation big = evaluateXvecLayout(filePath, elementWidth, ByteOrder.BIG_ENDIAN);

    if (!little.valid && !big.valid) {
      String reason = little.failureReason != null ? little.failureReason : big.failureReason;
      throw new IOException("Unable to interpret dimension header for file " + filePath
                            + (reason != null ? ": " + reason : ""));
    }

    return new EndianCheckResult(little, big);
  }

  private static EndianValidation evaluateXvecLayout(Path filePath, int elementWidth, ByteOrder order)
      throws IOException {

    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
      long fileSize = channel.size();
      if (fileSize < Integer.BYTES) {
        return EndianValidation.invalid("File too small to contain dimension header");
      }

      ByteBuffer dimBuffer = ByteBuffer.allocate(Integer.BYTES);
      int bytesRead = channel.read(dimBuffer, 0);
      if (bytesRead != Integer.BYTES) {
        return EndianValidation.invalid("Failed to read dimension header");
      }

      dimBuffer.flip();
      dimBuffer.order(order);
      int dimension = dimBuffer.getInt();

      if (dimension <= 0 || dimension > MAX_REASONABLE_DIMENSION) {
        return EndianValidation.invalid("Dimension " + dimension + " out of range");
      }

      long vectorBytes;
      long recordSize;
      try {
        vectorBytes = Math.multiplyExact((long) dimension, (long) elementWidth);
        recordSize = Math.addExact((long) Integer.BYTES, vectorBytes);
      } catch (ArithmeticException e) {
        return EndianValidation.invalid("Vector record size overflow");
      }

      if (recordSize <= Integer.BYTES) {
        return EndianValidation.invalid("Record size " + recordSize + " too small");
      }

      if (recordSize > fileSize) {
        return EndianValidation.invalid("Record size " + recordSize + " exceeds file length");
      }

      if (fileSize % recordSize != 0L) {
        return EndianValidation.invalid("File size " + fileSize + " is not a multiple of record size " + recordSize);
      }

      long vectorCount = fileSize / recordSize;
      if (vectorCount <= 0) {
        return EndianValidation.invalid("No vectors detected");
      }

      if (vectorCount > 1) {
        dimBuffer.clear();
        long lastOffset;
        try {
          lastOffset = Math.multiplyExact(vectorCount - 1, recordSize);
        } catch (ArithmeticException e) {
          return EndianValidation.invalid("Vector offset overflow");
        }

        int lastBytes = channel.read(dimBuffer, lastOffset);
        if (lastBytes != Integer.BYTES) {
          return EndianValidation.invalid("Failed to read trailing dimension");
        }
        dimBuffer.flip();
        dimBuffer.order(order);
        int trailingDimension = dimBuffer.getInt();
        if (trailingDimension != dimension) {
          return EndianValidation.invalid("Inconsistent dimension detected at final record: " + trailingDimension);
        }
      }

      return EndianValidation.valid(dimension, vectorCount);
    }
  }

  /// Result of inspecting little and big endian interpretations for an xvec file.
  public static final class EndianCheckResult {
    private final EndianValidation little;
    private final EndianValidation big;

    private EndianCheckResult(EndianValidation little, EndianValidation big) {
      this.little = little;
      this.big = big;
    }

    public boolean isLittleEndianValid() {
      return little.valid;
    }

    public boolean isBigEndianValid() {
      return big.valid;
    }

    public int getLittleEndianDimension() {
      return little.dimension;
    }

    public long getLittleEndianVectorCount() {
      return little.vectorCount;
    }

    public int getBigEndianDimension() {
      return big.dimension;
    }

    public long getBigEndianVectorCount() {
      return big.vectorCount;
    }

    public long getVectorCount() {
      return little.vectorCount;
    }

    public String getLittleEndianFailureReason() {
      return little.failureReason;
    }

    public String getBigEndianFailureReason() {
      return big.failureReason;
    }

    /// @return true if the file appears to use the opposite endian assumption from the standard little-endian layout
    public boolean isEndianMismatch() {
      return !little.valid && big.valid;
    }
  }

  private static final class EndianValidation {
    private final boolean valid;
    private final int dimension;
    private final long vectorCount;
    private final String failureReason;

    private EndianValidation(boolean valid, int dimension, long vectorCount, String failureReason) {
      this.valid = valid;
      this.dimension = dimension;
      this.vectorCount = vectorCount;
      this.failureReason = failureReason;
    }

    private static EndianValidation valid(int dimension, long vectorCount) {
      return new EndianValidation(true, dimension, vectorCount, null);
    }

    private static EndianValidation invalid(String reason) {
      return new EndianValidation(false, 0, 0L, reason);
    }
  }
}
