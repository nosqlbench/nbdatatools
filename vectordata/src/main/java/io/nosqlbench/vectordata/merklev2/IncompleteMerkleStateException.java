package io.nosqlbench.vectordata.merklev2;

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

/// Exception thrown when attempting to create a MerkleRef from an incomplete MerkleState.
/// This occurs when not all chunks have been validated and marked as valid in the state.
public class IncompleteMerkleStateException extends RuntimeException {
    
    private final int validChunkCount;
    private final int totalChunkCount;
    
    public IncompleteMerkleStateException(int validChunkCount, int totalChunkCount) {
        super(String.format("Cannot create MerkleRef from incomplete MerkleState. Only %d of %d chunks are valid.", 
              validChunkCount, totalChunkCount));
        this.validChunkCount = validChunkCount;
        this.totalChunkCount = totalChunkCount;
    }
    
    public int getValidChunkCount() {
        return validChunkCount;
    }
    
    public int getTotalChunkCount() {
        return totalChunkCount;
    }
}