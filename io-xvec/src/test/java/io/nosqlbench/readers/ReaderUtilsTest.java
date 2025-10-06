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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaderUtilsTest {

  @TempDir
  Path tempDir;

  @Test
  void computesVectorCountWithinRange() throws IOException {
    int recordSize = 68;
    long fileSize = recordSize * 100L;

    int count = ReaderUtils.computeVectorCount(Path.of("/tmp/test"), fileSize, recordSize, 0);

    assertEquals(100, count, "Vector count should be derived from file size / record size");
  }

  @Test
  void rejectsCountsExceedingIntegerRange() {
    int recordSize = 68;
    long fileSize = recordSize * (1L + Integer.MAX_VALUE);

    assertThrows(IOException.class,
        () -> ReaderUtils.computeVectorCount(Path.of("/tmp/too-large"), fileSize, recordSize, 0),
        "Vector counts beyond Integer.MAX_VALUE must be rejected");
  }

  @Test
  void rejectsNonPositiveRecordSize() {
    assertThrows(IOException.class,
        () -> ReaderUtils.computeVectorCount(Path.of("/tmp/invalid"), 100, 0, 0),
        "Non-positive record sizes must be rejected");
  }

  @Test
  void detectsStandardLittleEndianLayout() throws IOException {
    Path file = writeXvec(tempDir.resolve("little.fvec"), ByteOrder.LITTLE_ENDIAN, 4, 4);

    ReaderUtils.EndianCheckResult result = ReaderUtils.checkXvecEndianness(file, 4);

    assertTrue(result.isLittleEndianValid(), "Little-endian interpretation should be valid");
    assertFalse(result.isEndianMismatch(), "File should not be flagged as endian mismatch");
    assertEquals(4, result.getLittleEndianDimension(), "Dimension should match written value");
    assertEquals(1, result.getLittleEndianVectorCount(), "Vector count should match written value");
  }

  @Test
  void detectsEndianMismatchWhenHeaderIsBigEndian() throws IOException {
    Path file = writeXvec(tempDir.resolve("big.fvec"), ByteOrder.BIG_ENDIAN, 4, 4);

    ReaderUtils.EndianCheckResult result = ReaderUtils.checkXvecEndianness(file, 4);

    assertTrue(result.isBigEndianValid(), "Big-endian interpretation should be valid");
    assertTrue(result.isEndianMismatch(), "File should be flagged as endian mismatch");
    assertEquals(1, result.getBigEndianVectorCount(), "Vector count should match produced payload");
  }

  private Path writeXvec(Path file, ByteOrder headerOrder, int dimension, int elementWidth) throws IOException {
    try (OutputStream out = Files.newOutputStream(file)) {
      ByteBuffer header = ByteBuffer.allocate(4).order(headerOrder);
      header.putInt(dimension);
      out.write(header.array());

      byte[] payload = new byte[dimension * elementWidth];
      Arrays.fill(payload, (byte) 1);
      out.write(payload);
    }
    return file;
  }
}
