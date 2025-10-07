package io.nosqlbench.nbdatatools.api.transport;

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


import java.nio.ByteBuffer;

/**
 * Callback interface for handling partial data chunks with client-side tracking.
 * 
 * This interface allows transport clients to receive data incrementally as it becomes
 * available, with an associated tracking object that can be used to maintain client-side
 * state, correlate requests, or provide additional context for the data processing.
 * 
 * The generic type parameter T allows for flexible client-side tracking objects such as:
 * - Request identifiers
 * - Progress tracking objects  
 * - Client-specific metadata
 * - Error handling contexts
 * 
 * @param <T> The type of the client-side tracking object
 * 
 * @since 1.0
 */
@FunctionalInterface
public interface PartialData<T> {
    
    /**
     * Called when partial data is available for processing.
     * 
     * This method is invoked by the transport layer whenever a chunk of data
     * becomes available. The implementation should process the data appropriately
     * and can use the tracking object to maintain state or coordinate with other
     * operations.
     * 
     * @param data The partial data buffer. The buffer's position and limit define
     *             the valid data range. The buffer should not be modified by the
     *             implementation unless explicitly documented otherwise.
     * @param tracker The client-side tracking object associated with this data chunk.
     *                This object can be used to correlate this callback with the
     *                original request or maintain processing state.
     * 
     * @throws Exception If an error occurs while processing the data. The transport
     *                   layer may handle exceptions according to its error handling
     *                   policy, which could include retries, logging, or propagating
     *                   the exception to higher layers.
     */
    void onPartialData(ByteBuffer data, T tracker) throws Exception;
}
