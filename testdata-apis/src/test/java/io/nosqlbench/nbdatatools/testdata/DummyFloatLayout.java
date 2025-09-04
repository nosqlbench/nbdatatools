package io.nosqlbench.nbdatatools.testdata;

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


import java.util.Objects;

public class DummyFloatLayout {
  private final int chunksize;
  private final int vectorsPerSection;
  private final int dimensions;
  private final int sections;
  private final int totalVectors;

  public DummyFloatLayout(int chunksize, int vectorsPerSection, int dimensions, int sections, int totalVectors) {
    this.chunksize = chunksize;
    this.vectorsPerSection = vectorsPerSection;
    this.dimensions = dimensions;
    this.sections = sections;
    this.totalVectors = totalVectors;
  }

  public int chunksize() {
    return chunksize;
  }

  public int vectorsPerSection() {
    return vectorsPerSection;
  }

  public int dimensions() {
    return dimensions;
  }

  public int sections() {
    return sections;
  }

  public int totalVectors() {
    return totalVectors;
  }
  public static DummyFloatLayout forShape(int dimensions, int vectorsPerSection, int sections) {
    int reclen = Float.BYTES + (dimensions * Float.BYTES);
    int chunkSize = reclen * vectorsPerSection;
    return new DummyFloatLayout(chunkSize, vectorsPerSection, dimensions, sections, vectorsPerSection * sections);
  }

  public float[] generateGlobal(int index) {
    return generateForSection(index, index / vectorsPerSection);
  }

  public float[] generateForSection(int index, int section) {
    float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      vector[i] =
          (float) section + (float) i / 1000.0f;
    }
    vector[0]=section;
    vector[1]=index;
    vector[2]=(float) index / (float) vectorsPerSection;
    vector[3]=(float) (section*vectorsPerSection + index) / (float) totalVectors;
    return vector;
  }

  public float[][] generateAll() {
    float[][] vectors = new float[sections * vectorsPerSection][];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = generateGlobal(i);
    }
    return vectors;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DummyFloatLayout that = (DummyFloatLayout) o;
    return chunksize == that.chunksize && vectorsPerSection == that.vectorsPerSection &&
           dimensions == that.dimensions && sections == that.sections && totalVectors == that.totalVectors;
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunksize, vectorsPerSection, dimensions, sections, totalVectors);
  }

  @Override
  public String toString() {
    return "DummyFloatLayout{" +
           "chunksize=" + chunksize +
           ", vectorsPerSection=" + vectorsPerSection +
           ", dimensions=" + dimensions +
           ", sections=" + sections +
           ", totalVectors=" + totalVectors +
           '}';
  }
}
