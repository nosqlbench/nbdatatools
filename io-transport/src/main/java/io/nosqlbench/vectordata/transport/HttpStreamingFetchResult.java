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

import io.nosqlbench.nbdatatools.api.transport.StreamingFetchResult;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/// HTTP-specific implementation of StreamingFetchResult.
/// 
/// This class wraps an OkHttp Response and provides streaming access to the
/// response body through a ReadableByteChannel. It manages the lifecycle of
/// the HTTP response, ensuring proper cleanup when the result is closed.
/// 
/// The implementation is designed to handle large responses efficiently by
/// streaming data rather than buffering it all in memory.
public class HttpStreamingFetchResult implements StreamingFetchResult {
    
    private final Response response;
    private final ResponseBody responseBody;
    private final long offset;
    private final long requestedLength;
    private final String source;
    private final ReadableByteChannel channel;
    private volatile boolean closed = false;
    
    /// Creates a new HttpStreamingFetchResult.
    /// 
    /// @param response The OkHttp response (ownership is transferred to this object)
    /// @param responseBody The response body
    /// @param offset The offset from which the data was fetched
    /// @param requestedLength The number of bytes that were requested
    /// @param source The source URL
    public HttpStreamingFetchResult(Response response, ResponseBody responseBody, 
                                  long offset, long requestedLength, String source) {
        this.response = response;
        this.responseBody = responseBody;
        this.offset = offset;
        this.requestedLength = requestedLength;
        this.source = source;
        
        // Create channel from response body input stream
        InputStream inputStream = responseBody.byteStream();
        this.channel = Channels.newChannel(inputStream);
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
        // For HTTP responses, we can get the actual content length from headers
        String contentLength = response.header("Content-Length");
        if (contentLength != null) {
            try {
                return Long.parseLong(contentLength);
            } catch (NumberFormatException e) {
                // Fall through to default
            }
        }
        
        // Check Content-Range header for partial responses
        String contentRange = response.header("Content-Range");
        if (contentRange != null && contentRange.startsWith("bytes ")) {
            try {
                String rangeInfo = contentRange.substring("bytes ".length());
                String[] parts = rangeInfo.split("/");
                if (parts.length == 2 && parts[0].contains("-")) {
                    String[] rangeParts = parts[0].split("-");
                    if (rangeParts.length == 2) {
                        long start = Long.parseLong(rangeParts[0]);
                        long end = Long.parseLong(rangeParts[1]);
                        return end - start + 1;
                    }
                }
            } catch (Exception e) {
                // Fall through to default
            }
        }
        
        // If we can't determine from headers, return -1 (unknown)
        return -1;
    }
    
    @Override
    public boolean isSuccessful() {
        return !closed && response.isSuccessful();
    }
    
    @Override
    public String getSource() {
        return source;
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                channel.close();
            } finally {
                try {
                    responseBody.close();
                } finally {
                    response.close();
                }
            }
        }
    }
}