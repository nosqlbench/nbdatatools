package io.nosqlbench.vectordata.downloader;

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


import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JettyFileServerExtension.class)
public class ChunkedDownloaderTest {

    @Test
    public void directDownloadTest() {
      try {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Use the test server URL with the testxvec_base.fvec file
        URL fileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");

        ChunkedDownloader downloader = new ChunkedDownloader(
            fileUrl,
            "test",
            1024*1024, // 1MB chunks
            5, // 5 concurrent downloads
            new StdoutDownloadEventSink()
        );

        // Create a unique file name for this test
        String uniquePrefix = "test_" + UUID.randomUUID().toString().substring(0, 8);
        Path testout = Files.createTempFile(uniquePrefix, ".fvec");
        DownloadProgress download = downloader.download(testout, false);
        DownloadResult result = download.get();

        System.out.println("final progress:" + download);
        System.out.println("final result:" + result);

        // Verify the download was successful
        assertTrue(Files.exists(testout));
        assertEquals(10100000, Files.size(testout), "Downloaded file size should match the original");

        // Clean up
        Files.deleteIfExists(testout);
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /// Tests that the test web server supports ranged reads properly.
    ///
    /// This test verifies that the test web server correctly handles HTTP Range requests
    /// by making a request for a specific byte range and checking that:
    /// 1. The server responds with a 206 Partial Content status
    /// 2. The server returns the correct range of bytes
    /// 3. The server includes the appropriate headers (Accept-Ranges, Content-Range)
    @Test
    public void testServerSupportsRangedReads() throws IOException {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Use the test server URL with the testxvec_base.fvec file
        URL fileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");

        // Create an HTTP client
        OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();

        // Define the byte range to request (first 1000 bytes)
        long startByte = 0;
        long endByte = 999;
        String rangeHeader = "bytes=" + startByte + "-" + endByte;

        // Create a request with a Range header
        Request request = new Request.Builder()
            .url(fileUrl)
            .header("Range", rangeHeader)
            .build();

        // Execute the request
        try (Response response = client.newCall(request).execute()) {
            // Verify that the server responded with 206 Partial Content
            assertEquals(206, response.code(), "Server should respond with 206 Partial Content for range requests");

            // Verify that the server included the Accept-Ranges header
            String acceptRanges = response.header("Accept-Ranges");
            assertNotNull(acceptRanges, "Server should include Accept-Ranges header");
            assertEquals("bytes", acceptRanges, "Accept-Ranges header should be 'bytes'");

            // Verify that the server included the Content-Range header
            String contentRange = response.header("Content-Range");
            assertNotNull(contentRange, "Server should include Content-Range header");
            assertTrue(contentRange.startsWith("bytes " + startByte + "-" + endByte + "/"), 
                "Content-Range header should start with 'bytes " + startByte + "-" + endByte + "/'");

            // Verify that the response body has the correct length
            ResponseBody body = response.body();
            assertNotNull(body, "Response body should not be null");
            assertEquals(endByte - startByte + 1, body.contentLength(), 
                "Response body length should match the requested range length");

            // Read the response body to ensure it can be consumed without errors
            byte[] bytes = body.bytes();
            assertEquals(endByte - startByte + 1, bytes.length, 
                "Response body bytes length should match the requested range length");
        }
    }

    /// Tests that the ChunkDownloadTask can be created with a custom retry limit.
    ///
    /// This test verifies that:
    /// 1. The default retry limit is 10
    /// 2. A custom retry limit can be specified
    /// 3. The retry limit is correctly set in the ChunkDownloadTask
    @Test
    public void testChunkDownloadTaskRetryLimit() throws Exception {
        // Get the base URL from the JettyFileServerExtension
        URL baseUrl = JettyFileServerExtension.getBaseUrl();

        // Use the test server URL with the testxvec_base.fvec file
        URL fileUrl = new URL(baseUrl, "rawdatasets/testxvec/testxvec_base.fvec");

        // Create a temporary file
        Path tempFile = Files.createTempFile("retry-test", ".bin");

        // Create an HTTP client
        OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();

        // Create a ChunkDownloadTask with default retry limit
        ChunkDownloadTask defaultTask = new ChunkDownloadTask(
            client,
            fileUrl,
            tempFile,
            0,
            999,
            0,
            new AtomicLong(0),
            new AtomicBoolean(false),
            new StdoutDownloadEventSink()
        );

        // Create a ChunkDownloadTask with custom retry limit
        int customRetryLimit = 5;
        ChunkDownloadTask customTask = new ChunkDownloadTask(
            client,
            fileUrl,
            tempFile,
            0,
            999,
            0,
            new AtomicLong(0),
            new AtomicBoolean(false),
            new StdoutDownloadEventSink(),
            customRetryLimit
        );

        // Use reflection to access the private maxRetryAttempts field
        Field defaultField = ChunkDownloadTask.class.getDeclaredField("maxRetryAttempts");
        defaultField.setAccessible(true);
        int defaultRetryLimit = (int) defaultField.get(defaultTask);

        Field customField = ChunkDownloadTask.class.getDeclaredField("maxRetryAttempts");
        customField.setAccessible(true);
        int actualCustomRetryLimit = (int) customField.get(customTask);

        // Verify the default retry limit is 10
        assertEquals(10, defaultRetryLimit, "Default retry limit should be 10");

        // Verify the custom retry limit is set correctly
        assertEquals(customRetryLimit, actualCustomRetryLimit, "Custom retry limit should be set correctly");

        // Clean up
        Files.deleteIfExists(tempFile);
    }
}