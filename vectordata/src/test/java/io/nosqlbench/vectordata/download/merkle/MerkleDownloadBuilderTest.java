package io.nosqlbench.vectordata.download.merkle;

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
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class MerkleDownloadBuilderTest {
    @TempDir
    Path tempDir;

    @Test
    void demonstrateFluentApi() throws Exception {
        URL sourceUrl = new URL("https://jvector-datasets-public.s3.us-east-1.amazonaws.com/voyage-3-large-binary_d256_b10000_q10000_mk100.hdf5");
        Path targetPath = tempDir.resolve("downloaded-file.dat");

        CompletableFuture<MerkleDownloadResult> future = MerkleDownloadBuilder.forUrl(sourceUrl)
            .toPath(targetPath)
            .withTargetSpan(1024 * 1024 * 64) // 64MB span
            .useChunkedDownloader()
            .withChunkSize(1024 * 1024) // 1MB chunks
            .withMaxConcurrentChunks(4)
            .withRetries(3)
            .withTimeout(Duration.ofMinutes(5))
            .withRetryDelay(Duration.ofSeconds(1))
            .onProgress(progress -> {
                System.out.printf("Downloaded %d/%d bytes (%.2f%%)\n",
                    progress.bytesProcessed(),
                    progress.totalBytes(),
                    progress.progressPercent());
            })
            .onComplete(complete -> {
                if (complete.successful()) {
                    System.out.printf("Download completed in %dms\n", 
                        complete.timeElapsedMs());
                } else {
                    System.out.printf("Download failed: %s\n", 
                        complete.error().getMessage());
                }
            })
            .start();

        MerkleDownloadResult result = future.join();
        // Handle result...
    }
}
