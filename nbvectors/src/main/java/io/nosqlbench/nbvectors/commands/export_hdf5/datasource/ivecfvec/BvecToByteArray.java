package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec;

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


import io.nosqlbench.nbvectors.api.noncore.Enumerated;
import io.nosqlbench.nbvectors.api.noncore.Sized;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/// Read indexed float vectors from a file, incrementally
public class BvecToByteArray implements Iterable<byte[]>, Sized {

  private final Path path;
  private final int count;

  /// create a new FvecToIndexFloatVector
  /// @param path
  ///     the path to the file to read from
  public BvecToByteArray(Path path) {
    String[] parts = path.getFileName().toString().split("\\.");
    String extension = parts[parts.length - 1].toLowerCase();
    if (!extension.equals("bvec") && !extension.equals("bvecs")) {
      throw new RuntimeException("Unsupported file type: " + extension);
    }
    this.path = path;

    try (DataInputStream sizein = new DataInputStream(new BufferedInputStream(Files.newInputStream(
        path))))
    {
      int i = sizein.readInt();
      int dim = Integer.reverseBytes(i);
      int rowsize = (Byte.BYTES * dim) + Integer.BYTES;
      long filesize = Files.size(path);
      this.count = (int) (filesize / rowsize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }


  }

  /// {@inheritDoc}
  @Override
  public Iterator<byte[]> iterator() {
    return new IndicesIterable(this.path);
  }

  @Override
  public int getSize() {
    return count;
  }

  /// An iterator for indexed float vectors
  public static class IndicesIterable implements Iterator<byte[]>, Enumerated<byte[]> {

    private final DataInputStream in;
    private long lastIndex = -1;

    /// create a float iterable
    /// @param path
    ///     the path to the file to read from
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
    public byte[] next() {
      try {
        int dim = readLittleEndianInt(in);
//        System.out.println("dim:" + dim);
        byte[] vbuf = new byte[dim * Byte.BYTES];
        int read = in.read(vbuf);
        lastIndex++;
        return vbuf;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long getLastIndex() {
      return lastIndex;
    }
  }

  private static int readLittleEndianInt(DataInputStream dis) throws IOException {
    byte[] intBytes = new byte[4];
    dis.readFully(intBytes);
    // Wrap the byte array into a ByteBuffer, set the order to LITTLE_ENDIAN, then get the int.
    return ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

}
