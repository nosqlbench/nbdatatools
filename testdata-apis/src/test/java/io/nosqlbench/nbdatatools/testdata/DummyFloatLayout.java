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


public record DummyFloatLayout(
    int chunksize, int vectorsPerSection, int dimensions, int sections, int totalVectors
)
{
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
}
