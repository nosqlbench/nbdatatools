package io.nosqlbench.vectordata.access;

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
import io.nosqlbench.jetty.testserver.JettyFileServerFixture;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import io.nosqlbench.vectordata.transport.HttpByteRangeFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests for HTTP transport layer.
///
/// Mirrors test scenarios from vectordata-rs `tests/transport.rs`.
/// Tests HTTP Range requests, content-length detection, and byte-level fetch accuracy.
@ExtendWith(JettyFileServerExtension.class)
class TransportTest {

    private String dataUrl;

    @BeforeEach
    void setUp() throws Exception {
        JettyFileServerFixture server = JettyFileServerExtension.getServer();
        Path serverRoot = server.getRootDirectory();

        // Create 4096-byte test file with pattern byte[i] = i % 256
        byte[] content = new byte[4096];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 256);
        }
        Files.write(serverRoot.resolve("transport_test.bin"), content);
        dataUrl = JettyFileServerExtension.getBaseUrl() + "transport_test.bin";
    }

    /// Verifies fetching a range of bytes from the beginning of the file.
    @Test
    void testHttpTransportFetchRange() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(dataUrl)) {
            FetchResult<?> result = fetcher.fetchRange(0, 16).get();
            ByteBuffer buf = result.getData();

            assertThat(buf.remaining()).isEqualTo(16);
            for (int i = 0; i < 16; i++) {
                assertThat(buf.get()).isEqualTo((byte) (i % 256));
            }
        }
    }

    /// Verifies fetching a range from the middle of the file.
    @Test
    void testHttpTransportFetchRangeMiddle() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(dataUrl)) {
            FetchResult<?> result = fetcher.fetchRange(100, 50).get();
            ByteBuffer buf = result.getData();

            assertThat(buf.remaining()).isEqualTo(50);
            for (int i = 0; i < 50; i++) {
                assertThat(buf.get()).isEqualTo((byte) ((100 + i) % 256));
            }
        }
    }

    /// Verifies fetching the last bytes of the file.
    @Test
    void testHttpTransportFetchRangeEnd() throws Exception {
        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(dataUrl)) {
            FetchResult<?> result = fetcher.fetchRange(4064, 32).get();
            ByteBuffer buf = result.getData();

            assertThat(buf.remaining()).isEqualTo(32);
            for (int i = 0; i < 32; i++) {
                assertThat(buf.get()).isEqualTo((byte) ((4064 + i) % 256));
            }
        }
    }

    /// Verifies fetching xvec dimension header (first 4 bytes) via HTTP Range.
    @Test
    void testHttpFetchXvecDimensionHeader() throws Exception {
        JettyFileServerFixture server = JettyFileServerExtension.getServer();
        Path serverRoot = server.getRootDirectory();

        int dim = 4;
        int count = 10;
        byte[] content = TestVectorFileHelper.fvecBytes(dim, count);
        Files.write(serverRoot.resolve("transport_vectors.fvec"), content);
        String fvecUrl = JettyFileServerExtension.getBaseUrl() + "transport_vectors.fvec";

        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(fvecUrl)) {
            // Fetch first 4 bytes (dimension header)
            FetchResult<?> result = fetcher.fetchRange(0, 4).get();
            ByteBuffer buf = result.getData();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            int detectedDim = buf.getInt();
            assertThat(detectedDim).isEqualTo(dim);
        }
    }

    /// Verifies fetching a single vector record via HTTP Range request.
    @Test
    void testHttpFetchSingleVectorRecord() throws Exception {
        JettyFileServerFixture server = JettyFileServerExtension.getServer();
        Path serverRoot = server.getRootDirectory();

        int dim = 4;
        int count = 10;
        byte[] content = TestVectorFileHelper.fvecBytes(dim, count);
        Files.write(serverRoot.resolve("transport_vectors2.fvec"), content);
        String fvecUrl = JettyFileServerExtension.getBaseUrl() + "transport_vectors2.fvec";

        int recordSize = 4 + dim * 4;

        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(fvecUrl)) {
            // Fetch vector at index 5
            FetchResult<?> result = fetcher.fetchRange(5L * recordSize, recordSize).get();
            ByteBuffer buf = result.getData();
            buf.order(ByteOrder.LITTLE_ENDIAN);

            int recordDim = buf.getInt();
            assertThat(recordDim).isEqualTo(dim);

            // vector[5][j] = 5 * 4 + j = [20, 21, 22, 23]
            for (int j = 0; j < dim; j++) {
                assertThat(buf.getFloat()).isEqualTo((float) (5 * dim + j));
            }
        }
    }

    /// Verifies non-sequential (random access) Range requests work correctly.
    @Test
    void testHttpRandomAccessPattern() throws Exception {
        JettyFileServerFixture server = JettyFileServerExtension.getServer();
        Path serverRoot = server.getRootDirectory();

        int dim = 4;
        int count = 10;
        byte[] content = TestVectorFileHelper.fvecBytes(dim, count);
        Files.write(serverRoot.resolve("transport_random.fvec"), content);
        String fvecUrl = JettyFileServerExtension.getBaseUrl() + "transport_random.fvec";

        int recordSize = 4 + dim * 4;

        try (HttpByteRangeFetcher fetcher = new HttpByteRangeFetcher(fvecUrl)) {
            // Access vectors in non-sequential order: 7, 2, 9, 0
            int[] indices = {7, 2, 9, 0};
            for (int idx : indices) {
                FetchResult<?> result = fetcher.fetchRange((long) idx * recordSize, recordSize).get();
                ByteBuffer buf = result.getData();
                buf.order(ByteOrder.LITTLE_ENDIAN);
                buf.getInt(); // skip dim
                float firstElement = buf.getFloat();
                assertThat(firstElement).isEqualTo((float) (idx * dim));
            }
        }
    }
}
