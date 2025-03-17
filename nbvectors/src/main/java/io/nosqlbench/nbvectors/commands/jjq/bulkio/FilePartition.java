package io.nosqlbench.nbvectors.commands.jjq.bulkio;

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
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/// A FilePartition can be partitioned into a list of smaller FilePartitions.
/// When the size of the partitions would be too large for Java memory mapping,
/// then the number of partitions is automatically increased.
///
/// When there is less than the required amount of data in the provided partition
/// ([#partition(int, int)]) then no partitioning is done. This limit defaults to
/// 20MB for [#partition(int)]
/// @param path the path containing the current partition
/// @param start the starting point of the partition as an offset within the file
/// @param end the ending point of the partition as an offset within the file
/// @param id a labeling identifier, for debugging purposes

public record FilePartition(Path path, long start, long end, String id) {

  /// create a file partition
  /// @param path the path containing the current partition
  /// @return a file partition
  public static FilePartition of(String path) {
    return FilePartition.of(Path.of(path));
  }

  /// create a file partition
  /// @param path the path containing the current partition
  /// @return a file partition
  public static FilePartition of(Path path) {
    try {
      return new FilePartition(path, 0, Files.size(path), "0");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Break a file up into partitions on newline boundaries
  /// while ensuring that each boundary is less than the size
  /// @param partitions the number of partitions to create
  /// @return a list of file partitions
  public FilePartitions partition(int partitions) {
    return this.partition(partitions, 1 << 20);
  }

  /// Break a file up into partitions on newline boundaries
  /// while ensuring that each boundary is less than the size
  /// @param minPartitions the minimum number of partitions to create
  /// @param minSize the minimum size of each partition
  /// @return a list of file partitions
  public FilePartitions partition(int minPartitions, int minSize) {
    FilePartitions extents = new FilePartitions();

    long len = this.end - this.start;
    if (len <= minSize) {
      extents.add(this);
    } else {
      long partitions = Math.max(minPartitions, (Math.max(1, (len / 2000000000) + 1)));
      long psize = len / partitions;

      try {
        FileChannel channel = FileChannel.open(path);
        ByteBuffer buf = ByteBuffer.allocate(1 << 24);
        long pstart = start;

        for (long part = 0; part < partitions; part++) {
          buf.clear();
          long pend = Math.min(pstart + psize, channel.size());
          channel.position(pend);
          if (channel.position() < channel.size()) {
            int bytes = channel.read(buf);
            if (bytes >= 0) {
              buf.flip();
              int i = 0;
              for (i = 0; i < buf.limit(); i++) {
                if (buf.get(i) == '\n') {
                  pend = channel.position() - buf.limit() + i;
                  break;
                }
              }
              if (buf.get(i) != '\n') {
                throw new RuntimeException("oops again");
              }

            }
          }
          extents.add(new FilePartition(path, pstart, pend, id + String.format(":%03d", part)));
          pstart = pend;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return extents;

  }

  /// Map the file partition into a ByteBuffer
  /// @return a ByteBuffer
  public ByteBuffer mapFile() {
    long size = this.end - this.start;
    if (size > Integer.MAX_VALUE) {
      throw new RuntimeException(
          "File partition is too large to read into a ByteBuffer: " + size + " bytes");
    }
    if (size == 0) {
      throw new RuntimeException("File partition is empty");
    }
    ByteBuffer.allocate((int) size);
    try {
      FileChannel channel = FileChannel.open(path);
      MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, start, size);
      return buf;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.id).append(":");
    sb.append(this.path).append(":");
    sb.append(this.start).append("..");
    sb.append(this.end).append("[");
    ByteBuffer bb = this.mapFile();
    sb.append(StandardCharsets.UTF_8.decode(bb.slice(0, 15)).toString().replaceAll("\n", "\\\\n"));
    sb.append("..");
    sb.append(StandardCharsets.UTF_8.decode(bb.slice(bb.limit() - 15, 15)).toString()
        .replaceAll("\n", "\\\\n"));
    sb.append("]");
    return sb.toString();
  }

  private Iterable<String> asStringIterable() {
    ByteBuffer byteBuffer = mapFile();
    ConvertingIterable<CharBuffer, String> ci =
        new ConvertingIterable<>(
            new BytebufChunker(this.toString(), byteBuffer, 50000),
            Object::toString
        );
    FlatteningIterable<String, String> linesIter =
        new FlatteningIterable<>(ci, (String s) -> Arrays.asList(s.split("\n")));
    return linesIter;
  }

  /// Adapt this file partition into a concurrent supplier of lines
  /// @return a (thread-safe) concurrent supplier of lines
  public ConcurrentSupplier<String> asConcurrentSupplier() {
    return new ConcurrentSupplier<>(
        asStringIterable(), Runtime.getRuntime().availableProcessors() * 2, (e) -> {
      throw e;
    }
    );
  }
}
