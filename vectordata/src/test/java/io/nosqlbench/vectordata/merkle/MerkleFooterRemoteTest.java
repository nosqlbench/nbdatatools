package io.nosqlbench.vectordata.merkle;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MerkleFooter.fromRemoteUrl method.
 */
public class MerkleFooterRemoteTest {

    @TempDir
    Path tempDir;

    /**
     * Test that the fromRemoteUrl method correctly reads a footer from a remote URL.
     * This test uses a custom URL handler to simulate a remote server.
     */
    @Test
    void testFromRemoteUrl() throws Exception {
        // Create a test merkle tree file
        Path merkleFile = tempDir.resolve("test.mrkl");
        createTestMerkleFile(merkleFile);

        // Read the file to get the actual content
        byte[] fileContent = Files.readAllBytes(merkleFile);

        // Create a URL with a custom handler that returns our test data
        URL url = new URL("http", "example.com", 80, "/test.mrkl", new TestURLStreamHandler(fileContent, true));

        // Call the method under test
        MerkleFooter footer = MerkleFooter.fromRemoteUrl(url);

        // Verify the result
        assertNotNull(footer);
        assertEquals(16384, footer.chunkSize()); // 16KB chunk size
        assertEquals(1048576, footer.totalSize()); // 1MB total size
        assertEquals(MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE, footer.footerLength());
    }

    /**
     * Test that the fromRemoteUrl method correctly handles a server that doesn't support range requests.
     */
    @Test
    void testFromRemoteUrlWithoutRangeSupport() throws Exception {
        // Create a test merkle tree file
        Path merkleFile = tempDir.resolve("test.mrkl");
        createTestMerkleFile(merkleFile);

        // Read the file to get the actual content
        byte[] fileContent = Files.readAllBytes(merkleFile);

        // Create a URL with a custom handler that returns our test data but doesn't support range requests
        URL url = new URL("http", "example.com", 80, "/test.mrkl", new TestURLStreamHandler(fileContent, false));

        // Call the method under test
        MerkleFooter footer = MerkleFooter.fromRemoteUrl(url);

        // Verify the result
        assertNotNull(footer);
        assertEquals(16384, footer.chunkSize()); // 16KB chunk size
        assertEquals(1048576, footer.totalSize()); // 1MB total size
        assertEquals(MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE, footer.footerLength());
    }

    /**
     * Test that the fromRemoteUrl method correctly handles errors.
     */
    @Test
    void testFromRemoteUrlWithErrors() throws Exception {
        // Create a URL with a custom handler that simulates an error
        URL url = new URL("http", "example.com", 80, "/test.mrkl", new ErrorURLStreamHandler());

        try {
            // Call the method under test
            MerkleFooter.fromRemoteUrl(url);
            fail("Expected IOException");
        } catch (IOException e) {
            // Expected exception
            assertTrue(e.getMessage().contains("Failed to get file size"));
        }
    }

    /**
     * Creates a test merkle tree file with a valid footer.
     */
    private void createTestMerkleFile(Path merkleFile) throws IOException {
        // Create a merkle tree with a 16KB chunk size and 1MB total size
        // Create a root node with a zero hash
        byte[] zeroHash = new byte[MerkleNode.HASH_SIZE];
        MerkleNode root = MerkleNode.leaf(0, zeroHash);
        MerkleTree tree = new MerkleTree(root, 16384, 1048576, new MerkleRange(0, 1048576));

        // Save the tree to the file
        tree.save(merkleFile);
    }

    /**
     * A custom URL stream handler that returns predefined content.
     */
    private static class TestURLStreamHandler extends URLStreamHandler {
        private final byte[] content;
        private final boolean supportsRanges;

        public TestURLStreamHandler(byte[] content, boolean supportsRanges) {
            this.content = content;
            this.supportsRanges = supportsRanges;
        }

        @Override
        protected URLConnection openConnection(URL url) {
            return new TestHttpURLConnection(url, content, supportsRanges);
        }
    }

    /**
     * A custom HttpURLConnection that returns predefined content.
     */
    private static class TestHttpURLConnection extends HttpURLConnection {
        private final byte[] content;
        private final boolean supportsRanges;
        private String requestMethod = "GET";
        private String rangeHeader = null;

        public TestHttpURLConnection(URL url, byte[] content, boolean supportsRanges) {
            super(url);
            this.content = content;
            this.supportsRanges = supportsRanges;
        }

        @Override
        public void setRequestMethod(String method) {
            this.requestMethod = method;
        }

        @Override
        public void setRequestProperty(String key, String value) {
            if ("Range".equalsIgnoreCase(key)) {
                rangeHeader = value;
            }
        }

        @Override
        public int getResponseCode() {
            if ("HEAD".equals(requestMethod)) {
                return HTTP_OK;
            } else if (rangeHeader != null && supportsRanges) {
                return HTTP_PARTIAL;
            } else {
                return HTTP_OK;
            }
        }

        @Override
        public long getContentLengthLong() {
            return content.length;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            if (rangeHeader != null && supportsRanges) {
                // Parse range header (format: "bytes=start-end")
                String[] parts = rangeHeader.substring(6).split("-");
                int start = Integer.parseInt(parts[0]);
                int end = Integer.parseInt(parts[1]);
                int length = end - start + 1;

                byte[] rangeContent = new byte[length];
                System.arraycopy(content, start, rangeContent, 0, length);
                return new ByteArrayInputStream(rangeContent);
            } else {
                return new ByteArrayInputStream(content);
            }
        }

        @Override
        public void disconnect() {
            // No-op
        }

        @Override
        public boolean usingProxy() {
            return false;
        }

        @Override
        public void connect() {
            // No-op
        }
    }

    /**
     * A custom URL stream handler that simulates errors.
     */
    private static class ErrorURLStreamHandler extends URLStreamHandler {
        @Override
        protected URLConnection openConnection(URL url) {
            return new ErrorHttpURLConnection(url);
        }
    }

    /**
     * A custom HttpURLConnection that simulates errors.
     */
    private static class ErrorHttpURLConnection extends HttpURLConnection {
        public ErrorHttpURLConnection(URL url) {
            super(url);
        }

        @Override
        public int getResponseCode() {
            return HTTP_NOT_FOUND;
        }

        @Override
        public void disconnect() {
            // No-op
        }

        @Override
        public boolean usingProxy() {
            return false;
        }

        @Override
        public void connect() {
            // No-op
        }
    }
}
