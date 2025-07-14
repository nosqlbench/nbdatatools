package io.nosqlbench.vectordata.merkle.tasks;

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

/// Descriptor for a chunk to be processed by a ChunkWorker
public class ChunkDescriptor {
    /// chunk index
    public final int chunkIndex;

    /// Creates a new chunk descriptor
    /// @param chunkIndex The index of the chunk
    public ChunkDescriptor(int chunkIndex) {
        this.chunkIndex = chunkIndex;
    }
}