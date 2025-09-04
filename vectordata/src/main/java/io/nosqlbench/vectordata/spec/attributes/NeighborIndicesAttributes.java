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


/// This class captures attribute requirements for the neighbor indices dataset
public class NeighborIndicesAttributes {
    /// the number of query vectors
    private final long count;
    /// the number of neighbors provided for each query vector
    private final long max_k;
    
    public NeighborIndicesAttributes(long count, long max_k) {
        this.count = count;
        this.max_k = max_k;
    }
    
    /// @return the number of query vectors
    public long count() {
        return count;
    }
    
    /// @return the number of neighbors provided for each query vector
    public long max_k() {
        return max_k;
    }
}
