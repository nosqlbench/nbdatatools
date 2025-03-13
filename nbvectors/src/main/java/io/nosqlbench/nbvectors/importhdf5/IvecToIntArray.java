package io.nosqlbench.nbvectors.importhdf5;

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


import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/// Read indexed float vectors from a file, incrementally
public class IvecToIntArray implements Iterable<int[]> {

  private final Path path;

  /// create a new FvecToIndexFloatVector
  /// @param path the path to the file to read from
  public IvecToIntArray(Path path) {
    String[] parts = path.getFileName().toString().split("\\.");
    String extension = parts[parts.length - 1].toLowerCase();
    if (!extension.equals("ivec")) {
      throw new RuntimeException("Unsupported file type: " + extension);
    }
    this.path = path;
  }

  /// {@inheritDoc}
  @Override
  public Iterator<int[]> iterator() {
    return new IndicesIterable(this.path);
  }

  /// An iterator for indexed float vectors
  public static class IndicesIterable implements Iterator<int[]> {

    private final DataInputStream in;
    private long index = 0;

    /// create a float iterable
    /// @param path the path to the file to read from
    public IndicesIterable(Path path) {
      try {
        this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      try {
        return (in.available() > 0);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int[] next() {
      try {
        int dim = Integer.reverseBytes(in.readInt());
        byte[] vbuf = new byte[dim*Integer.BYTES];
        int read = in.read(vbuf);
        IntBuffer fbuf= ByteBuffer.wrap(vbuf).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int[] ary = new int[dim];
        fbuf.get(ary);
        return ary;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
