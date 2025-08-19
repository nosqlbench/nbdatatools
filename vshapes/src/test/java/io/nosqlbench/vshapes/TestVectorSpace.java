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

package io.nosqlbench.vshapes;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Test implementation of VectorSpace for unit testing.
 */
public class TestVectorSpace implements VectorSpace {
    
    private final List<float[]> vectors = new ArrayList<>();
    private final List<Integer> classLabels = new ArrayList<>();
    private final String id;
    
    public TestVectorSpace() {
        this("test-vector-space");
    }
    
    public TestVectorSpace(String id) {
        this.id = id;
    }
    
    public void addVector(float[] vector) {
        addVector(vector, null);
    }
    
    public void addVector(float[] vector, Integer classLabel) {
        vectors.add(vector.clone());
        classLabels.add(classLabel);
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    @Override
    public int getVectorCount() {
        return vectors.size();
    }
    
    @Override
    public int getDimension() {
        return vectors.isEmpty() ? 0 : vectors.get(0).length;
    }
    
    @Override
    public float[] getVector(int index) {
        if (index < 0 || index >= vectors.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + vectors.size());
        }
        return vectors.get(index).clone();
    }
    
    @Override
    public float[][] getAllVectors() {
        float[][] result = new float[vectors.size()][];
        for (int i = 0; i < vectors.size(); i++) {
            result[i] = vectors.get(i).clone();
        }
        return result;
    }
    
    @Override
    public boolean hasClassLabels() {
        return classLabels.stream().anyMatch(label -> label != null);
    }
    
    @Override
    public Optional<Integer> getClassLabel(int index) {
        if (index < 0 || index >= classLabels.size()) {
            return Optional.empty();
        }
        return Optional.ofNullable(classLabels.get(index));
    }
    
    /**
     * Creates a test vector space with known characteristics for testing.
     * @return test vector space
     */
    public static TestVectorSpace createTestSpace() {
        TestVectorSpace space = new TestVectorSpace("known-test-space");
        
        // Create vectors with known distances and relationships
        space.addVector(new float[]{0.0f, 0.0f}, 0);      // Origin, class 0
        space.addVector(new float[]{1.0f, 0.0f}, 0);      // Distance 1 from origin, class 0
        space.addVector(new float[]{0.0f, 1.0f}, 0);      // Distance 1 from origin, class 0
        space.addVector(new float[]{3.0f, 4.0f}, 1);      // Distance 5 from origin, class 1
        space.addVector(new float[]{-1.0f, -1.0f}, 1);    // Distance sqrt(2) from origin, class 1
        
        return space;
    }
    
    /**
     * Creates a higher-dimensional test space.
     * @return high-dimensional test vector space
     */
    public static TestVectorSpace createHighDimTestSpace() {
        TestVectorSpace space = new TestVectorSpace("high-dim-test");
        
        // Create 10-dimensional vectors
        for (int i = 0; i < 20; i++) {
            float[] vector = new float[10];
            for (int d = 0; d < 10; d++) {
                vector[d] = (float) (Math.random() * 10 - 5); // Random values [-5, 5]
            }
            space.addVector(vector, i % 3); // 3 classes
        }
        
        return space;
    }
}