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


import io.nosqlbench.vectordata.status.StdoutDownloadEventSink;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

public class ChunkedDownloaderTest {

    @Disabled("direct example")
    @Test
    public void directDownloadTest() {
      try {
        ChunkedDownloader downloader = new ChunkedDownloader(
            new URL("https://jvector-datasets-public.s3.us-east-1.amazonaws.com/voyage-3-large-binary_d256_b10000_q10000_mk100.hdf5"),
            "test",
            1024*1024*16,
            10,
            new StdoutDownloadEventSink()
        );
          Path testout = Files.createTempFile("test", ".hdf5");
        DownloadProgress download = downloader.download(testout, false);
        DownloadResult result = download.get();
        System.out.println("final progress:" +download);
        System.out.println("final result:"+result);
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
