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

import java.util.concurrent.CompletableFuture;

/**
 * A merkle painter is a helper which uses a merkle tree to download and verify data.
 * It is given a reference to a MerkleState, which is used to track the validity of the data.
 * The MerkleState is used to determine which chunks need to be downloaded and verified.
 * The MerklePainter is responsible for downloading the data and updating the MerkleState.
 * It is also responsible for retrying failed downloads and verifying the data against
 * the reference merkle tree, which is provided by the MerkleState.
 * MerklePainter instances have an instance of an AsynchronousFileChannel provided, which is
 * used to store the downloaded data. They also have a reference to a ChunkedTransportClient,
 * which is used to download the data. The MerkleState is used with io.nosqlbench.vectordata
 * .merklev2.MerkleState#saveIfValid(int, java.nio.ByteBuffer, java.util.function.Consumer) to
 * verify and store the data.
 */
public interface MerklePainter {
    
    /**
     * Ensures that the specified range is available and verified.
     * Downloads and verifies any missing chunks in the range.
     * 
     * @param startPosition Start position (inclusive)
     * @param endPosition End position (exclusive)  
     * @return CompletableFuture that completes when range is available
     */
    CompletableFuture<Void> ensureRange(long startPosition, long endPosition);
    
    /**
     * Gets the merkle shape for this painter.
     * 
     * @return The merkle shape
     */
    MerkleShape getShape();
    
    /**
     * Closes the painter and releases resources.
     */
    void close();
}
