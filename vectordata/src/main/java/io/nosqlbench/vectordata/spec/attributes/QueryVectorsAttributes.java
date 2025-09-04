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


/// This class captures attribute requirements for the query vectors dataset
public class QueryVectorsAttributes {
    /// the name of the model used to generate the data
    private final String model;
    /// the number of query vectors
    private final long count;
    /// the number of dimensions in each query vector
    private final int dimensions;
    
    public QueryVectorsAttributes(String model, long count, int dimensions) {
        this.model = model;
        this.count = count;
        this.dimensions = dimensions;
    }
    
    /// @return the name of the model used to generate the data
    public String model() {
        return model;
    }
    
    /// @return the number of query vectors
    public long count() {
        return count;
    }
    
    /// @return the number of dimensions in each query vector
    public int dimensions() {
        return dimensions;
    }
}
