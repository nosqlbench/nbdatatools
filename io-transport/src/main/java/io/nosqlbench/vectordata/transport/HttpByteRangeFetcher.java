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

import io.nosqlbench.nbdatatools.api.services.TransportScheme;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import io.nosqlbench.nbdatatools.api.transport.StreamingFetchResult;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/// HTTP-based implementation of ByteRangeFetcher using OkHttp client.
/// 
/// This implementation provides efficient range-based access to HTTP/HTTPS resources
/// using the HTTP Range header. It maintains a persistent HTTP client with connection
/// pooling for optimal performance across multiple requests.
/// 
/// Key features:
/// - Supports HTTP Range requests for partial content retrieval
/// - Thread-safe concurrent access
/// - Connection pooling and keepalive for performance
/// - Automatic retry handling for transient network errors
/// - Content-Length detection for size information
/// - Proper resource cleanup and connection management
/// 
/// The implementation uses OkHttp's async capabilities to provide non-blocking
/// operations while maintaining thread safety for concurrent access patterns.
@TransportScheme({"http", "https"})
public class HttpByteRangeFetcher implements ChunkedTransportClient {

    /// HTTP client instance with optimized configuration for range requests
    private final OkHttpClient httpClient;
    
    /// The source URL being fetched from
    private final String sourceUrl;
    
    /// Cached size of the remote resource (null if not yet determined)
    private final AtomicReference<Long> cachedSize = new AtomicReference<>();
    
    /// Cached flag indicating if server supports range requests (null if not yet determined)
    private final AtomicReference<Boolean> cachedRangeSupport = new AtomicReference<>();
    
    /// Flag to track if this fetcher has been closed
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /// Creates a new HTTP byte range fetcher for the specified URL.
    /// 
    /// @param url The HTTP or HTTPS URL to fetch data from
    /// @throws IllegalArgumentException if the URL is null or not an HTTP/HTTPS URL
    public HttpByteRangeFetcher(String url) {
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("URL cannot be null or empty");
        }
        
        String trimmedUrl = url.trim();
        if (!trimmedUrl.toLowerCase().startsWith("http://") && 
            !trimmedUrl.toLowerCase().startsWith("https://")) {
            throw new IllegalArgumentException("URL must be HTTP or HTTPS: " + url);
        }
        
        this.sourceUrl = trimmedUrl;
        this.httpClient = createOptimizedHttpClient();
    }


    /// Creates an optimized OkHttp client configured for aggressive bandwidth usage and range requests.
    /// 
    /// This configuration includes:
    /// - Large connection pool for aggressive connection reuse
    /// - Increased dispatcher limits for parallel requests
    /// - Socket-level performance tuning
    /// - HTTP/2 support for multiplexing
    /// 
    /// @return A configured OkHttpClient instance optimized for high bandwidth
    private OkHttpClient createOptimizedHttpClient() {
        // Create dispatcher with aggressive limits
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(200);  // Increase from default 64
        dispatcher.setMaxRequestsPerHost(50);  // Increase from default 5
        
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
            // Aggressive connection pooling
            .connectionPool(new ConnectionPool(
                100,  // maxIdleConnections - keep many connections alive
                5,    // keepAliveDuration
                TimeUnit.MINUTES
            ))
            
            // Use custom dispatcher
            .dispatcher(dispatcher);
        
        return builder
            // Timeouts optimized for high bandwidth
            .connectTimeout(10, TimeUnit.SECONDS)  // Shorter connect timeout
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(30, TimeUnit.SECONDS)
            
            // Enable HTTP/2 for multiplexing
            .protocols(Arrays.asList(Protocol.HTTP_2, Protocol.HTTP_1_1))
            
            // Retry on connection failure
            .retryOnConnectionFailure(true)
            
            .build();
    }

    @Override
    public CompletableFuture<? extends FetchResult<?>> fetchRange(long offset, long length) throws IOException {
        validateNotClosed();
        
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Build range request
                long endOffset = offset + length - 1;
                Request request = new Request.Builder()
                    .url(sourceUrl)
                    .addHeader("Range", "bytes=" + offset + "-" + endOffset)
                    .build();

                // Execute request
                try (Response response = httpClient.newCall(request).execute()) {
                    validateResponse(response, offset, length);
                    
                    ResponseBody body = response.body();
                    if (body == null) {
                        throw new IOException("Response body is null");
                    }

                    // Read response data into ByteBuffer
                    byte[] data = body.bytes();
                    if (data.length != length) {
                        throw new IOException("Expected " + length + " bytes but received " + data.length);
                    }

                    return new FetchResult<>(ByteBuffer.wrap(data), offset, length);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to fetch range (HttpByteRangeFetcher1) [" + offset + "-" + (offset + length - 1) + "]", e);
            }
        });
    }

    @Override
    public CompletableFuture<StreamingFetchResult> fetchRangeStreaming(long offset, long length) throws IOException {
        validateNotClosed();
        
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Build range request
                long endOffset = offset + length - 1;
                Request request = new Request.Builder()
                    .url(sourceUrl)
                    .addHeader("Range", "bytes=" + offset + "-" + endOffset)
                    .build();

                // Execute request asynchronously and return streaming result
                Response response = httpClient.newCall(request).execute();
                
                try {
                    validateResponse(response, offset, length);
                    
                    ResponseBody body = response.body();
                    if (body == null) {
                        response.close();
                        throw new IOException("Response body is null");
                    }

                    // Create streaming result that wraps the response
                    return new HttpStreamingFetchResult(response, body, offset, length, sourceUrl);
                    
                } catch (Exception e) {
                    // Clean up response on error
                    response.close();
                    throw e;
                }
            } catch (IOException e) {
                throw new CompletionException("Failed to fetch range (HttpByteRangeFetcher2) [" + offset + "-" + (offset + length - 1) + "]", e);
            }
        });
    }
    
    @Override
    public CompletableFuture<Long> getSize() throws IOException {
        validateNotClosed();
        
        // Return cached size if available
        Long cached = cachedSize.get();
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Use HEAD request to get content length
                Request headRequest = new Request.Builder()
                    .url(sourceUrl)
                    .head()
                    .build();

                try (Response response = httpClient.newCall(headRequest).execute()) {
                    if (!response.isSuccessful()) {
                        throw new IOException("HEAD request failed with status: " + response.code());
                    }

                    String contentLength = response.header("Content-Length");
                    if (contentLength == null) {
                        throw new IOException("Server did not provide Content-Length header");
                    }

                    long size = Long.parseLong(contentLength);
                    cachedSize.set(size);
                    
                    // Also check for range support while we have the response
                    String acceptRanges = response.header("Accept-Ranges");
                    cachedRangeSupport.set("bytes".equals(acceptRanges));
                    
                    return size;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to determine content size", e);
            }
        });
    }

    @Override
    public boolean supportsRangeRequests() {
        // Return cached result if available
        Boolean cached = cachedRangeSupport.get();
        if (cached != null) {
            return cached;
        }

        // If not cached, make a HEAD request to check
        try {
            Request headRequest = new Request.Builder()
                .url(sourceUrl)
                .head()
                .build();

            try (Response response = httpClient.newCall(headRequest).execute()) {
                String acceptRanges = response.header("Accept-Ranges");
                boolean supportsRanges = "bytes".equals(acceptRanges);
                cachedRangeSupport.set(supportsRanges);
                return supportsRanges;
            }
        } catch (IOException e) {
            // If we can't determine range support, assume false
            cachedRangeSupport.set(false);
            return false;
        }
    }

    @Override
    public String getSource() {
        return sourceUrl;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            // Close the HTTP client's connection pool and executor service
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    /// Validates that the HTTP response is appropriate for a range request.
    /// 
    /// @param response The HTTP response to validate
    /// @param requestedOffset The offset that was requested
    /// @param requestedLength The length that was requested
    /// @throws IOException if the response is invalid
    private void validateResponse(Response response, long requestedOffset, long requestedLength) throws IOException {
        if (response.code() == 206) {
            // Partial Content - this is what we expect for range requests
            validateContentRange(response, requestedOffset, requestedLength);
        } else if (response.code() == 200) {
            // OK - server doesn't support ranges but returned full content
            // We'll read only the requested portion
            validateFullContentResponse(response, requestedOffset, requestedLength);
        } else if (response.code() == 416) {
            throw new IOException("Requested range not satisfiable: " + response.header("Content-Range"));
        } else {
            throw new IOException("HTTP request failed with status: " + response.code() + " " + response.message());
        }
    }

    /// Validates the Content-Range header for partial content responses.
    /// 
    /// @param response The HTTP response with status 206
    /// @param requestedOffset The offset that was requested
    /// @param requestedLength The length that was requested
    /// @throws IOException if the content range is invalid
    private void validateContentRange(Response response, long requestedOffset,
        long requestedLength) throws IOException {
        String contentRange = response.header("Content-Range");
        if (contentRange == null) {
            throw new IOException("Server returned 206 but no Content-Range header");
        }

        // Parse Content-Range header (format: "bytes start-end/total")
        if (!contentRange.startsWith("bytes ")) {
            throw new IOException("Invalid Content-Range header format: " + contentRange);
        }

        String rangeInfo = contentRange.substring("bytes ".length());
        String[] parts = rangeInfo.split("/");
        if (parts.length != 2) {
            throw new IOException("Invalid Content-Range header format: " + contentRange);
        }

        String[] rangeParts = parts[0].split("-");
        if (rangeParts.length != 2) {
            throw new IOException("Invalid Content-Range header format: " + contentRange);
        }

        long start = Long.parseLong(rangeParts[0]);
        long end = Long.parseLong(rangeParts[1]);

        if (start != requestedOffset) {
            throw new IOException("Server returned different start offset. Expected: " + requestedOffset + ", Got: " + start);
        }

        long expectedEnd = requestedOffset + requestedLength - 1;
        if (end != expectedEnd) {
            throw new IOException("Server returned different end offset. Expected: " + expectedEnd + ", Got: " + end);
        }
    }

    /// Validates a full content response when range requests aren't supported.
    /// 
    /// @param response The HTTP response with status 200
    /// @param requestedOffset The offset that was requested
    /// @param requestedLength The length that was requested
    /// @throws IOException if the response cannot satisfy the range request
    private void validateFullContentResponse(Response response, long requestedOffset, long requestedLength) throws IOException {
        String contentLength = response.header("Content-Length");
        if (contentLength != null) {
            long totalSize = Long.parseLong(contentLength);
            if (requestedOffset + requestedLength > totalSize) {
                throw new IOException("Requested range exceeds content size. Size: " + totalSize + 
                                    ", Requested: " + requestedOffset + "-" + (requestedOffset + requestedLength - 1));
            }
        }
        // Note: We'll handle extracting the requested portion in the calling method
    }

    /// Validates that this fetcher has not been closed.
    /// 
    /// @throws IOException if the fetcher has been closed
    private void validateNotClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("HttpByteRangeFetcher has been closed");
        }
    }


}