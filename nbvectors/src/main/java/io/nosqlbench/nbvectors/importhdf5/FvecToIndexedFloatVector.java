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


import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/// Read indexed float vectors from a file, incrementally
public class FvecToIndexedFloatVector implements Iterable<LongIndexedFloatVector> {

  private final Path path;

  /// create a new FvecToIndexFloatVector
  /// @param path the path to the file to read from
  public FvecToIndexedFloatVector(Path path) {
    String[] parts = path.getFileName().toString().split("\\.");
    String extension = parts[parts.length - 1].toLowerCase();
    if (!extension.equals("fvec")) {
      throw new RuntimeException("Unsupported file type: " + extension);
    }
    this.path = path;
  }

  /// {@inheritDoc}
  @Override
  public Iterator<LongIndexedFloatVector> iterator() {
    return new FloatIterable(this.path);
  }

  /// An iterator for indexed float vectors
  public static class FloatIterable implements Iterator<LongIndexedFloatVector> {

    private final DataInputStream in;
    private long index = 0;

    /// create a float iterable
    /// @param path the path to the file to read from
    public FloatIterable(Path path) {
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
    public LongIndexedFloatVector next() {
      try {
        int i = in.readInt();
        int dim = Integer.reverseBytes(i);
        byte[] vbuf = new byte[dim*Float.BYTES];
        int read = in.read(vbuf);
        if (read!=vbuf.length) {
          throw new RuntimeException("read " + read + " bytes, expected " + vbuf.length);
        }
        FloatBuffer fbuf= ByteBuffer.wrap(vbuf).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
        float[] ary = new float[dim];
        fbuf.get(ary);
        return new LongIndexedFloatVector(index++, ary);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
