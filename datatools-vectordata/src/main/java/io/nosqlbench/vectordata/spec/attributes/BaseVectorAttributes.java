package io.nosqlbench.vectordata.spec.attributes;

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


import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;

/// This class captures the basic metadata requirements of the vector test data spec
/// @see DistanceFunction
public class BaseVectorAttributes {
    /// the number of vectors in the dataset
    private final long count;
    /// the number of dimensions in each vector  
    private final long dimensions;
    /// the name of the model used to generate the data
    private final String model;
    /// the distance function used to compute distance between vectors
    private final DistanceFunction distance_function;
    
    public BaseVectorAttributes(long count, long dimensions, String model, DistanceFunction distance_function) {
        this.count = count;
        this.dimensions = dimensions;
        this.model = model;
        this.distance_function = distance_function;
    }
    
    /// @return the number of vectors in the dataset
    public long count() {
        return count;
    }
    
    /// @return the number of dimensions in each vector
    public long dimensions() {
        return dimensions;
    }
    
    /// @return the name of the model used to generate the data
    public String model() {
        return model;
    }
    
    /// @return the distance function used to compute distance between vectors
    public DistanceFunction distance_function() {
        return distance_function;
    }
}
