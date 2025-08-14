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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Unit tests for ByteRangeFetcherFactory.
@ExtendWith(JettyFileServerExtension.class)
public class ByteRangeFetcherFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    void testCreateHttpFetcher() throws IOException {
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String httpUrl = baseUrl.toString() + "rawdatasets/testxvec/testxvec_base.fvec";

        ChunkedTransportClient fetcher = ChunkedTransportIO.create(httpUrl);
        
        assertNotNull(fetcher);
        assertInstanceOf(HttpByteRangeFetcher.class, fetcher);
        assertEquals(httpUrl, fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateHttpsFetcher() throws IOException {
        String httpsUrl = "https://example.com/data.bin";

        ChunkedTransportClient fetcher = ChunkedTransportIO.create(httpsUrl);
        
        assertNotNull(fetcher);
        assertInstanceOf(HttpByteRangeFetcher.class, fetcher);
        assertEquals(httpsUrl, fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateFileFetcher() throws IOException {
        // Create a test file
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, new byte[]{1, 2, 3, 4, 5});

        ChunkedTransportClient fetcher = ChunkedTransportIO.create(testFile.toString());
        
        assertNotNull(fetcher);
        assertInstanceOf(FileByteRangeFetcher.class, fetcher);
        assertEquals(testFile.toUri().toString(), fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateFileUrlFetcher() throws IOException {
        // Create a test file
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, new byte[]{1, 2, 3, 4, 5});
        
        String fileUrl = testFile.toUri().toString();
        ChunkedTransportClient fetcher = ChunkedTransportIO.create(fileUrl);
        
        assertNotNull(fetcher);
        assertInstanceOf(FileByteRangeFetcher.class, fetcher);
        assertEquals(fileUrl, fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateFromURI() throws IOException {
        // Create a test file
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, new byte[]{1, 2, 3, 4, 5});
        
        URI fileUri = testFile.toUri();
        ChunkedTransportClient fetcher = ChunkedTransportIO.create(fileUri);
        
        assertNotNull(fetcher);
        assertInstanceOf(FileByteRangeFetcher.class, fetcher);
        assertEquals(fileUri.toString(), fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateFromPath() throws IOException {
        // Create a test file
        Path testFile = tempDir.resolve("test.dat");
        Files.write(testFile, new byte[]{1, 2, 3, 4, 5});

        ChunkedTransportClient fetcher = ChunkedTransportIO.create(testFile);
        
        assertNotNull(fetcher);
        assertInstanceOf(FileByteRangeFetcher.class, fetcher);
        assertEquals(testFile.toUri().toString(), fetcher.getSource());
        
        fetcher.close();
    }

    @Test
    void testCreateWithNullUrl() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create((String) null);
        });
    }

    @Test
    void testCreateWithEmptyUrl() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create("");
        });
    }

    @Test
    void testCreateWithNullUri() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create((URI) null);
        });
    }

    @Test
    void testCreateWithNullPath() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create((Path) null);
        });
    }

    @Test
    void testCreateWithUnsupportedScheme() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create("ftp://example.com/file.dat");
        });
    }

    @Test
    void testCreateWithNonExistentFile() {
        Path nonExistentFile = tempDir.resolve("nonexistent.dat");
        
        assertThrows(IOException.class, () -> {
            ChunkedTransportIO.create(nonExistentFile.toString());
        });
    }

    @Test
    void testCreateWithDirectory() throws IOException {
        Path directory = tempDir.resolve("testdir");
        Files.createDirectory(directory);
        
        assertThrows(IOException.class, () -> {
            ChunkedTransportIO.create(directory.toString());
        });
    }
}