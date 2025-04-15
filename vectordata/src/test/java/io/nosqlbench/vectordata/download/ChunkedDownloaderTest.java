package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.download.chunker.ChunkedDownloader;
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