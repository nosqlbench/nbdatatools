package io.nosqlbench.vectordata;

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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Arrays;

public class GenHdf5TestData {

  @Disabled
  @Test
  public void genFile1() {

    WritableHdfFile writable = HdfFile.write(Paths.get("numbers.hdf5"));

    int[] irow = new int[1000];
    for (int r = 0; r < irow.length; r++) {
      irow[r]=r;
    }
    int[][] icols = new int[1000][];
    for (int i = 0; i < icols.length; i++) {
      icols[i]=Arrays.copyOf(irow,irow.length);
      icols[i][0]=i;
    }
    writable.putDataset("ints_1000_1000", icols);

    float[] frow = new float[1000];
    for (int r = 0; r < frow.length; r++) {
      frow[r]=r;
    }
    float[][] fcols = new float[1000][];
    for (int i = 0; i < fcols.length; i++) {
      fcols[i]=Arrays.copyOf(frow,frow.length);
      fcols[i][0]=i;
    }

    writable.putDataset("floats_1000_1000", fcols);


    byte[] brow = new byte[1000];
    for (int i = 0; i < brow.length; i++) {
      brow[i]=(byte)(i%255);
    }
    byte[][] bcols = new byte[1000][];
    for (int i = 0; i < bcols.length; i++) {
      bcols[i]=Arrays.copyOf(brow,brow.length);
      bcols[i][0]=(byte)i;
    }
    writable.putDataset("bytes_1000_1000", bcols);

    double[] drow = new double[1000];
    for (int i = 0; i < drow.length; i++) {
      drow[i]=(double)i;
    }
    double[][] dcols = new double[1000][];
    for (int i = 0; i < dcols.length; i++) {
      dcols[i]=Arrays.copyOf(drow,drow.length);
      dcols[i][0]=(double)i;
    }
    writable.putDataset("doubles_1000_1000", dcols);
    writable.close();

  }


}
