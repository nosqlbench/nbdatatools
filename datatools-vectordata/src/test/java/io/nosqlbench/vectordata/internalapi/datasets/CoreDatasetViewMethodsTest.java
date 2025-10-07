package io.nosqlbench.vectordata.internalapi.datasets;

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
import io.jhdf.api.Dataset;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.layout.FWindow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.assertj.core.api.Assertions.assertThat;

// Made the class public
public class CoreDatasetViewMethodsTest {

  private static HdfFile hdf;

  private static Dataset ints;
  private static Dataset bytes;
  private static Dataset floats;
  private static Dataset doubles;

  private static CoreDatasetViewFixture<float[]> floatsView;
  private static CoreDatasetViewFixture<int[]> intsView;
  private static CoreDatasetViewFixture<byte[]> bytesView;
  private static CoreDatasetViewFixture<double[]> doublesView;
  private static CoreDatasetViewFixture<byte[]> bytesView33;
  private static CoreDatasetViewFixture<int[]> intsView42;
  private static CoreDatasetViewFixture<float[]> floatsView77;
  private static CoreDatasetViewFixture<double[]> doublesView101;

  // Made the method public
  @BeforeAll
  public static void setUpAll() {
    URL resource = getSystemClassLoader().getResource("numbers.hdf5");
    try {
      Path path = Path.of(resource.toURI().getPath());
      hdf = new HdfFile(path);

      ints = hdf.getDatasetByPath("/ints_1000_1000");
      bytes = hdf.getDatasetByPath("/bytes_1000_1000");
      floats = hdf.getDatasetByPath("/floats_1000_1000");
      doubles = hdf.getDatasetByPath("/doubles_1000_1000");

      intsView = new CoreDatasetViewFixture<>(ints, FWindow.ALL);
      bytesView = new CoreDatasetViewFixture<>(bytes, FWindow.ALL);
      floatsView = new CoreDatasetViewFixture<>(floats, FWindow.ALL);
      doublesView = new CoreDatasetViewFixture<>(doubles, FWindow.ALL);

      bytesView33 = new CoreDatasetViewFixture<>(bytes, new FWindow("33..1000"));
      intsView42 = new CoreDatasetViewFixture<>(ints, new FWindow("42..1000"));
      floatsView77 = new CoreDatasetViewFixture<>(floats, new FWindow("77..1000"));
      doublesView101 = new CoreDatasetViewFixture<>(doubles, new FWindow("101..1000"));

    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  // Made the method public
  @Test
  public void getCount() {
    assertThat(ints.getDimensions()[0]).isEqualTo(1000);
    assertThat(intsView.getCount()).isEqualTo(1000);

    assertThat(intsView42.getCount()).isEqualTo(958);
    assertThat(bytesView33.getCount()).isEqualTo(967);
    assertThat(floatsView77.getCount()).isEqualTo(923);
    assertThat(doublesView101.getCount()).isEqualTo(899);
  }

  // Made the method public
  @Test
  public void getVectorDimensions() {
    int vectorDimensions = intsView.getVectorDimensions();
    assertThat(vectorDimensions).isEqualTo(1000);

    int vd2 = doublesView101.getVectorDimensions();
    assertThat(vd2).isEqualTo(899);

  }

  // Made the method public
  @Test
  public void getBaseType() {
    assertThat(intsView.getDataType()).isEqualTo(int.class);
    assertThat(bytesView.getDataType()).isEqualTo(byte.class);
    assertThat(floatsView.getDataType()).isEqualTo(float.class);

    assertThat(doublesView101.getDataType()).isEqualTo(double.class);
  }

  // Made the method public
  @Test
  public void getFloatVector() {
    float[] fv0 = floatsView.getFloatVector(5);
    assertThat(Arrays.copyOfRange(fv0, 0, 10)).isEqualTo(new float[]{
        5.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });

    float[] fv77 = floatsView77.getFloatVector(10);
    assertThat(Arrays.copyOfRange(fv77, 0, 10)).isEqualTo(new float[]{
        87.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });
  }

  // Made the method public
  @Test
  public void getVector() {
    float[] vector = floatsView.getVector(3);
    float[] v0 = floatsView.getFloatVector(3);
    assertThat(vector).isEqualTo(v0);
  }

  // Made the method public
  @Test
  public void getVectors() {
    float[][] vs510 = floatsView.getVectors(5, 10);
    assertThat(Arrays.copyOfRange(vs510[0], 0, 10)).isEqualTo(new float[]{
        5.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });
    assertThat(Arrays.copyOfRange(vs510[4], 0, 10)).isEqualTo(new float[]{
        9.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });
  }

  // Made the method public
  @Test
  public void getDoubleVector() {
    double[] dv0 = doublesView.getDoubleVector(0);
    assertThat(Arrays.copyOfRange(dv0, 0, 10)).isEqualTo(new double[]{
        0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0
    });
  }

  // Made the method public
  @Test
  public void getIndexedObject() {
    assertThat(intsView.getIndexedObject(45).index()).isEqualTo(45);
    assertThat(intsView.getIndexedObject(45).value()[0]).isEqualTo(45);
  }

  // Made the method public
  @Test
  public void getDataRange() {
    Object[] idr310 = intsView.getDataRange(3, 10);
    assertThat(idr310.length).isEqualTo(7);
    assertThat(Arrays.copyOfRange((int[]) idr310[0], 0, 10)).isEqualTo(new int[]{3,1,2,3,4,5,6,7,8,9});
  }

  // Made the method public
  @Test
  public void getRawObject() {
    Object i0 = intsView.getRawElement(15);
    assertThat(Array.get(i0, 0)).isEqualTo(15);
    assertThat(Array.get(i0, 12)).isEqualTo(12);
    assertThat(Array.get(i0, 999)).isEqualTo(999);
  }

  // Made the method public
  @Test
  public void slice() {
    float[] f0 = floatsView.slice(16);
    assertThat(Arrays.copyOfRange(f0, 0, 10)).isEqualTo(new float[]{
        16.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });
    float[] f077 = floatsView77.slice(16);
    assertThat(Arrays.copyOfRange(f077, 0, 10)).isEqualTo(new float[]{
        93.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f
    });
  }

  // Made the method public
  @Test
  public void sliceIndexed1D() {
    Indexed<int[]>[] indexed = intsView.sliceIndexed1D(9, 10);
    assertThat(indexed.length).isEqualTo(1);
    assertThat(Arrays.copyOfRange(indexed[0].value(), 0, 10)).isEqualTo(new int[]{
        9, 1, 2, 3, 4, 5, 6, 7, 8, 9
    });
    assertThat(indexed[0].index()).isEqualTo(9);
  }

  // Made the method public
  @Test
  public void sliceRange() {
    int[][] ints37 = intsView.sliceRange(3, 7);
    assertThat(ints37.length).isEqualTo(4);
    assertThat(Arrays.copyOfRange(ints37[0], 0, 10)).isEqualTo(new int[]{
        3, 1, 2, 3, 4, 5, 6, 7, 8, 9
    });
  }

  // Made the method public
  @Test
  public void get() {
    int[] i0 = intsView.get(90);
    assertThat(Array.get(i0, 0)).isEqualTo(90);
    assertThat(Array.get(i0, 999)).isEqualTo(999);
  }

  // Made the method public
  @Test
  public void getRange() {
    int[][] ints1113 = intsView.getRange(11, 13);
    assertThat(ints1113.length).isEqualTo(2);
    assertThat(Arrays.copyOfRange(ints1113[0], 0, 10)).isEqualTo(new int[]{
        11, 1, 2, 3, 4, 5, 6, 7, 8, 9
    });
    assertThat(Arrays.copyOfRange(ints1113[1], 0, 10)).isEqualTo(new int[]{
        12, 1, 2, 3, 4, 5, 6, 7, 8, 9
    });
  }

  // Made the method public
  @Test
  public void getIndexed() {
    Indexed<int[]> ind21 = intsView.getIndexed(21);
    assertThat(ind21.index()).isEqualTo(21);
  }

  // Made the method public
  @Test
  public void getIndexedRange() {
    Indexed<int[]>[] iranged = intsView.getIndexedRange(2, 10);
    assertThat(iranged.length).isEqualTo(8);
    assertThat(iranged[0].index()).isEqualTo(2);
    assertThat(iranged[7].index()).isEqualTo(9);
  }

  // Made the method public
  @Test
  public void testToList() {
    List<Integer> list1 = intsView.toList(v -> v[0]);
    assertThat(list1.size()).isEqualTo(1000);

    List<Integer> list2 = intsView.toList(v -> v[3]);
    assertThat(list2.size()).isEqualTo(1000);
  }

}
