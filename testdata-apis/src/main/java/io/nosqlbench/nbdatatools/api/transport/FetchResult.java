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

/// Wrapper type for fetch results that allows extensibility through generic subtypes.
/// 
/// This base type provides the core functionality of wrapping a ByteBuffer result
/// from a fetch operation. Implementations can extend this type to add additional
/// metadata such as progress tracking, timing information, or other transport-specific
/// details.
/// 
/// The generic type parameter T allows implementations to return their own subtypes
/// while maintaining type safety through the self-referential generic pattern.
/// 
/// Example usage:
/// ```java
/// // Basic usage
/// FetchResult<FetchResult<?>> result = client.fetchRange(0, 1024).get();
/// ByteBuffer data = result.getData();
/// 
/// // Extended usage with progress tracking
/// ProgressTrackingFetchResult progress = service.fetchRange(0, 1024).get();
/// ByteBuffer data = progress.getData();
/// long bytesTransferred = progress.getBytesTransferred();
/// ```
public class FetchResult<T extends FetchResult<T>> {
    
    private final ByteBuffer data;
    private final long offset;
    private final long requestedLength;
    
    /// Creates a new FetchResult with the specified data and request parameters.
    /// 
    /// @param data The fetched data as a ByteBuffer
    /// @param offset The offset from which the data was fetched
    /// @param requestedLength The number of bytes that were requested
    public FetchResult(ByteBuffer data, long offset, long requestedLength) {
        this.data = data;
        this.offset = offset;
        this.requestedLength = requestedLength;
    }
    
    /// Gets the fetched data.
    /// 
    /// @return The data as a ByteBuffer
    public ByteBuffer getData() {
        return data;
    }
    
    /// Gets the offset from which the data was fetched.
    /// 
    /// @return The offset in bytes
    public long getOffset() {
        return offset;
    }
    
    /// Gets the number of bytes that were requested.
    /// 
    /// @return The requested length in bytes
    public long getRequestedLength() {
        return requestedLength;
    }
    
    /// Gets the actual number of bytes fetched.
    /// 
    /// This may be less than the requested length if the end of the resource
    /// was reached or if the server returned fewer bytes than requested.
    /// 
    /// @return The actual number of bytes in the data buffer
    public int getActualLength() {
        return data != null ? data.remaining() : 0;
    }
    
    /// Checks if the fetch was successful.
    /// 
    /// A fetch is considered successful if data was returned, even if it's
    /// less than the requested amount (which can happen at the end of a file).
    /// 
    /// @return true if data was successfully fetched, false otherwise
    public boolean isSuccessful() {
        return data != null;
    }
    
    /// Returns this instance cast to the concrete type.
    /// 
    /// This method supports the self-referential generic pattern, allowing
    /// subtypes to maintain their type identity through method chains.
    /// 
    /// @return This instance cast to type T
    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }
}