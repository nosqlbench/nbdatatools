package io.nosqlbench.vectordata.transport;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/// A ReadableByteChannel wrapper that limits the number of bytes that can be read.
/// 
/// This class wraps another ReadableByteChannel and ensures that no more than
/// a specified number of bytes can be read from it. This is useful for implementing
/// range-based reads where we want to prevent reading beyond the requested range.
/// 
/// The channel keeps track of bytes read and returns EOF (-1) once the limit
/// is reached, even if the underlying channel has more data available.
public class LimitedReadableByteChannel implements ReadableByteChannel {
    
    private final ReadableByteChannel delegate;
    private final long limit;
    private long bytesRead = 0;
    private boolean open = true;
    
    /// Creates a new LimitedReadableByteChannel.
    /// 
    /// @param delegate The underlying channel to read from
    /// @param limit The maximum number of bytes that can be read
    public LimitedReadableByteChannel(ReadableByteChannel delegate, long limit) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate channel cannot be null");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("Limit cannot be negative");
        }
        this.delegate = delegate;
        this.limit = limit;
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!open) {
            throw new IOException("Channel is closed");
        }
        
        // Check if we've reached the limit
        if (bytesRead >= limit) {
            return -1; // EOF
        }
        
        // Calculate how many bytes we can read without exceeding the limit
        long remaining = limit - bytesRead;
        if (remaining == 0) {
            return -1; // EOF
        }
        
        // Limit the buffer if necessary
        int originalLimit = dst.limit();
        if (remaining < dst.remaining()) {
            // Temporarily reduce the buffer limit to not exceed our byte limit
            // Handle case where remaining bytes might exceed int range
            if (remaining > Integer.MAX_VALUE) {
                // If remaining is >2GB, limit to current buffer capacity to avoid overflow
                dst.limit(dst.position() + Math.min(dst.remaining(), Integer.MAX_VALUE));
            } else {
                dst.limit(dst.position() + (int) remaining);
            }
        }
        
        try {
            // Read from the delegate
            int bytesReadNow = delegate.read(dst);
            
            if (bytesReadNow > 0) {
                bytesRead += bytesReadNow;
            }
            
            return bytesReadNow;
            
        } finally {
            // Restore original buffer limit
            dst.limit(originalLimit);
        }
    }
    
    @Override
    public boolean isOpen() {
        return open && delegate.isOpen();
    }
    
    @Override
    public void close() throws IOException {
        open = false;
        // Note: We don't close the delegate channel as it may be shared
        // The owner of the delegate is responsible for closing it
    }
    
    /// Gets the number of bytes read so far.
    /// 
    /// @return The number of bytes read through this channel
    public long getBytesRead() {
        return bytesRead;
    }
    
    /// Gets the remaining number of bytes that can be read.
    /// 
    /// @return The number of bytes remaining before the limit is reached
    public long getBytesRemaining() {
        return Math.max(0, limit - bytesRead);
    }
}