package io.nosqlbench.vectordata.merkle;

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


/**
 * A simplified version of DummyFloatLayout for testing purposes.
 * This class generates dummy float data with a specific layout.
 */
public record DummyFloatLayout(
    int chunksize, int vectorsPerSection, int dimensions, int sections, int totalVectors
) {
  /**
   * Creates a DummyFloatLayout with the specified shape.
   *
   * @param dimensions The number of dimensions for each vector
   * @param vectorsPerSection The number of vectors per section
   * @param sections The number of sections
   * @return A new DummyFloatLayout
   */
  public static DummyFloatLayout forShape(int dimensions, int vectorsPerSection, int sections) {
    int reclen = Float.BYTES + (dimensions * Float.BYTES);
    int chunkSize = reclen * vectorsPerSection;
    return new DummyFloatLayout(chunkSize, vectorsPerSection, dimensions, sections, vectorsPerSection * sections);
  }

  /**
   * Generates a vector for a global index.
   *
   * @param index The global index
   * @return A float array representing the vector
   */
  public float[] generateGlobal(int index) {
    return generateForSection(index % vectorsPerSection, index / vectorsPerSection);
  }

  /**
   * Generates a vector for a specific section and index.
   *
   * @param index The index within the section
   * @param section The section number
   * @return A float array representing the vector
   */
  public float[] generateForSection(int index, int section) {
    float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      // Make each dimension value depend on both section and index to ensure uniqueness
      vector[i] = (float) section + (float) i / 1000.0f + (float) index / (1000.0f * vectorsPerSection);
    }
    // These values are more prominently different for easier debugging
    vector[0] = section;
    vector[1] = index;
    vector[2] = (float) index / (float) vectorsPerSection;
    vector[3] = (float) (section * vectorsPerSection + index) / (float) totalVectors;
    return vector;
  }

  /**
   * Generates all vectors for all sections.
   *
   * @return A 2D array of floats representing all vectors
   */
  public float[][] generateAll() {
    float[][] vectors = new float[sections * vectorsPerSection][];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = generateGlobal(i);
    }
    return vectors;
  }
}
