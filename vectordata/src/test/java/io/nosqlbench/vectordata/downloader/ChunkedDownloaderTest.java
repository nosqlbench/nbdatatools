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


import io.nosqlbench.vectordata.downloader.testserver.TestWebServerExtension;
import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChunkedDownloaderTest {

    @Test
    public void directDownloadTest() {
      try {
        // Get the base URL from the TestWebServerExtension
        URL baseUrl = TestWebServerExtension.getBaseUrl();

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
}
