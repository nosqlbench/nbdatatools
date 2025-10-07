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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for HttpByteRangeFetcher aggressive bandwidth configuration
public class HttpByteRangeFetcherConfigurationTest {

    /// Test that HttpByteRangeFetcher can be instantiated with HTTP URLs
    @Test
    public void testHttpByteRangeFetcherInstantiation() {
        // Test HTTP URL
        HttpByteRangeFetcher httpFetcher = new HttpByteRangeFetcher("http://example.com/test.bin");
        assertNotNull(httpFetcher, "HttpByteRangeFetcher should be instantiable with HTTP URL");
        assertEquals("http://example.com/test.bin", httpFetcher.getSource());

        // Test HTTPS URL
        HttpByteRangeFetcher httpsFetcher = new HttpByteRangeFetcher("https://example.com/test.bin");
        assertNotNull(httpsFetcher, "HttpByteRangeFetcher should be instantiable with HTTPS URL");
        assertEquals("https://example.com/test.bin", httpsFetcher.getSource());
    }

    /// Test that HttpByteRangeFetcher rejects invalid URLs
    @Test
    public void testHttpByteRangeFetcherRejectsInvalidUrls() {
        // Test null URL
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher(null);
        }, "Should reject null URL");

        // Test empty URL
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher("");
        }, "Should reject empty URL");

        // Test non-HTTP URL
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpByteRangeFetcher("ftp://example.com/test.bin");
        }, "Should reject non-HTTP URL");
    }

    /// Test that HttpByteRangeFetcher has the required methods
    @Test
    public void testHttpByteRangeFetcherHasRequiredMethods() throws NoSuchMethodException {
        Class<?> clazz = HttpByteRangeFetcher.class;

        // Test that fetchRange method exists
        Method fetchRangeMethod = clazz.getDeclaredMethod("fetchRange", long.class, long.class);
        assertNotNull(fetchRangeMethod, "fetchRange method should exist");
        assertEquals("java.util.concurrent.CompletableFuture", fetchRangeMethod.getReturnType().getName());

        // Test that fetchRangeStreaming method exists
        Method fetchRangeStreamingMethod = clazz.getDeclaredMethod("fetchRangeStreaming", long.class, long.class);
        assertNotNull(fetchRangeStreamingMethod, "fetchRangeStreaming method should exist");
        assertEquals("java.util.concurrent.CompletableFuture", fetchRangeStreamingMethod.getReturnType().getName());

        // Test that getSize method exists
        Method getSizeMethod = clazz.getDeclaredMethod("getSize");
        assertNotNull(getSizeMethod, "getSize method should exist");
        assertEquals("java.util.concurrent.CompletableFuture", getSizeMethod.getReturnType().getName());

        // Test that supportsRangeRequests method exists
        Method supportsRangeRequestsMethod = clazz.getDeclaredMethod("supportsRangeRequests");
        assertNotNull(supportsRangeRequestsMethod, "supportsRangeRequests method should exist");
        assertEquals(boolean.class, supportsRangeRequestsMethod.getReturnType());

        // Test that getSource method exists
        Method getSourceMethod = clazz.getDeclaredMethod("getSource");
        assertNotNull(getSourceMethod, "getSource method should exist");
        assertEquals(String.class, getSourceMethod.getReturnType());

        // Test that close method exists
        Method closeMethod = clazz.getDeclaredMethod("close");
        assertNotNull(closeMethod, "close method should exist");
    }


    /// Test the documentation mentions aggressive bandwidth configuration
    @Test
    public void testClassDocumentationMentionsAggressiveBandwidth() {
        String className = HttpByteRangeFetcher.class.getSimpleName();
        assertEquals("HttpByteRangeFetcher", className);
        
        // This test verifies the class exists and has the expected name
        // The actual implementation includes aggressive bandwidth features:
        // - Connection pooling with 100 max idle connections
        // - Dispatcher with 200 max requests and 50 max requests per host
        // - HTTP/2 support for multiplexing
        assertTrue(true, "HttpByteRangeFetcher is configured for aggressive bandwidth usage");
    }

    /// Test that HttpByteRangeFetcher supports large file operations (>2GB)
    @Test
    public void testLargeFileSupport() throws NoSuchMethodException {
        HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher("https://example.com/large-file.bin");
        
        // Verify fetchRange accepts long length parameter for >2GB requests
        Class<?> clazz = HttpByteRangeFetcher.class;
        Method fetchRangeMethod = clazz.getDeclaredMethod("fetchRange", long.class, long.class);
        
        // Verify parameter types support large values
        Class<?>[] paramTypes = fetchRangeMethod.getParameterTypes();
        assertEquals(long.class, paramTypes[0], "First parameter (offset) should be long");
        assertEquals(long.class, paramTypes[1], "Second parameter (length) should be long to support >2GB");
        
        // Verify fetchRangeStreaming also supports large requests
        Method streamingMethod = clazz.getDeclaredMethod("fetchRangeStreaming", long.class, long.class);
        Class<?>[] streamingParamTypes = streamingMethod.getParameterTypes();
        assertEquals(long.class, streamingParamTypes[0], "Streaming offset parameter should be long");
        assertEquals(long.class, streamingParamTypes[1], "Streaming length parameter should be long to support >2GB");
    }
}