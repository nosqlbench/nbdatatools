/*
 * This file is part of jHDF. A pure Java library for accessing HDF5 files.
 *
 * https://jhdf.io
 *
 * Copyright (c) 2025 James Mudd
 *
 * MIT License see 'LICENSE' file
 */
package io.nosqlbench.nbvectors.common.jhdf;

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


import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.exceptions.HdfWritingException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamableDatasetImplTest {

  @Disabled
  @Test
  public void testDimensionChecking() {
    Path hdf5Out;
    try {
      hdf5Out = Files.createTempFile(
          Paths.get("."), // defaulting to /tmp isn't great for large files testing
          this.getClass().getSimpleName() + "_BiggerThan2GbB_dataset", ".hdf5"
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Integer> chunkIdx = List.of(0);
    int chunkRows = (1024 * 1024) / Long.BYTES;
    int rowsize = 1024;

    IterableSource<Integer, long[][]> sf =
        new IterableSource<>(chunkIdx, i -> getArrayData(i, rowsize, chunkRows));

    WritableHdfFile out = HdfFile.write(hdf5Out);
    StreamableDataset sd = new StreamableDatasetImpl(sf, "", out);
    sd.modifyDimensions(new int[]{(chunkRows * chunkIdx.size()) + 1, rowsize});
    out.putDataset("testname", sd);
//    out.close();
    assertThrows(HdfWritingException.class, out::close);
  }


  @Disabled
  @Test
  public void testBasicStreaming() {

    System.out.println("memory: " + Runtime.getRuntime().maxMemory());

    Path hdf5Out;
    try {
      hdf5Out = Files.createTempFile(
          Paths.get("."), // defaulting to /tmp isn't great for large files testing
          this.getClass().getSimpleName() + "_BiggerThan2GbB_dataset", ".hdf5"
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<Integer> chunkIdx = Arrays.asList(0, 1, 2, 3);
    int chunkRows = (1024 * 1024) / Long.BYTES;
    int rowsize = 1024;

    IterableSource<Integer, long[][]> sf =
        new IterableSource<>(chunkIdx, i -> getArrayData(i, rowsize, chunkRows));

    try (WritableHdfFile out = HdfFile.write(hdf5Out)) {
      StreamableDataset sd = new StreamableDatasetImpl(sf, "", out);

      assertThrows(HdfWritingException.class, sd::getData);
      assertThrows(HdfWritingException.class, sd::getDimensions);
      assertThrows(HdfWritingException.class, sd::getDataFlat);
      assertThrows(HdfWritingException.class, sd::getDataType);
      assertThrows(HdfWritingException.class, sd::getJavaType);

      sd.enableCompute();
      assertThat(sd.getDimensions()).isNotNull();
      assertThat(sd.getJavaType()).isNotNull();
      assertThat(sd.getDataType()).isNotNull();
      assertThat(sd.getSize()).isNotNull();
      assertThat(sd.getSizeInBytes()).isNotNull();

      sd.modifyDimensions(new int[]{chunkRows * chunkIdx.size(), rowsize});
      out.putDataset("testname", sd);
    }
  }

  private long[][] getArrayData(long offset, int rowsize, int rows) {
    long[][] data = new long[rows][rowsize];
    for (int i = 0; i < data.length; i++) {
      long[] row = new long[rowsize];
      Arrays.fill(row, offset + i);
      data[i] = row;
    }
    return data;
  }

  public static final class IterableSource<I, O> implements Iterable<O> {

    private final List<I> list;
    private final Iterator<I> iter;
    private final Function<I, O> f;

    IterableSource(List<I> in, Function<I, O> f) {
      this.list = in;
      this.iter = list.iterator();
      this.f = f;
    }

    @Override
    public Iterator<O> iterator() {
      return new Iter(this.list.iterator(), this.f);
    }

    public final static class Iter<I, O> implements Iterator<O> {
      private final Function<I, O> f;
      private final Iterator<I> iter;

      public Iter(Iterator<I> iterI, Function<I, O> f) {
        this.f = f;
        this.iter = iterI;
      }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public O next() {
        return f.apply(iter.next());
      }
    }
  }
}
