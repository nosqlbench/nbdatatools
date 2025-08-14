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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/// Adapter that wraps a ByteBuffer as a StreamingFetchResult.
/// 
/// This class provides backward compatibility by allowing ByteBuffer-based
/// results to be used as streaming results. It implements ReadableByteChannel
/// internally to expose the ByteBuffer data through the streaming interface.
/// 
/// This is primarily used as a default implementation for the streaming API
/// when true streaming support is not available in the underlying transport.
public class ByteBufferStreamingResult implements StreamingFetchResult {
    
    private final ByteBuffer data;
    private final long offset;
    private final long requestedLength;
    private final String source;
    private final ByteBufferChannel channel;
    private volatile boolean closed = false;
    
    /// Creates a new ByteBufferStreamingResult.
    /// 
    /// @param data The ByteBuffer containing the fetched data
    /// @param offset The offset from which the data was fetched
    /// @param requestedLength The number of bytes that were requested
    /// @param source The source identifier
    public ByteBufferStreamingResult(ByteBuffer data, long offset, long requestedLength, String source) {
        this.data = data;
        this.offset = offset;
        this.requestedLength = requestedLength;
        this.source = source;
        this.channel = new ByteBufferChannel(data);
    }
    
    @Override
    public ReadableByteChannel getDataChannel() {
        if (closed) {
            throw new IllegalStateException("StreamingFetchResult has been closed");
        }
        return channel;
    }
    
    @Override
    public long getOffset() {
        return offset;
    }
    
    @Override
    public long getRequestedLength() {
        return requestedLength;
    }
    
    @Override
    public long getActualLength() {
        return data != null ? data.remaining() : 0;
    }
    
    @Override
    public boolean isSuccessful() {
        return data != null && !closed;
    }
    
    @Override
    public String getSource() {
        return source;
    }
    
    @Override
    public void close() throws IOException {
        closed = true;
        channel.close();
    }
    
    /// Internal ReadableByteChannel implementation that reads from a ByteBuffer.
    private static class ByteBufferChannel implements ReadableByteChannel {
        private final ByteBuffer source;
        private volatile boolean open = true;
        
        ByteBufferChannel(ByteBuffer source) {
            this.source = source;
        }
        
        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new IOException("Channel is closed");
            }
            
            if (!source.hasRemaining()) {
                return -1; // EOF
            }
            
            int bytesToRead = Math.min(dst.remaining(), source.remaining());
            if (bytesToRead == 0) {
                return 0;
            }
            
            // Save source limit
            int oldLimit = source.limit();
            
            // Set temporary limit to read only what fits in dst
            source.limit(source.position() + bytesToRead);
            
            // Transfer data
            dst.put(source);
            
            // Restore original limit
            source.limit(oldLimit);
            
            return bytesToRead;
        }
        
        @Override
        public boolean isOpen() {
            return open;
        }
        
        @Override
        public void close() throws IOException {
            open = false;
        }
    }
}