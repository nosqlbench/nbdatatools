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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MerkleTree.sync method.
 */
public class MerkleTreeSyncTest {

    @TempDir
    Path tempDir;

    /**
     * Test that the sync method correctly downloads a file when it doesn't exist locally.
     */
    @Test
    void testSyncWhenFileDoesNotExist() throws Exception {
        // Create test data
        byte[] fileContent = createRandomData(1024 * 1024); // 1MB
        byte[] merkleContent = createMerkleFileContent(fileContent, 16384); // 16KB chunks

        // Create URLs with custom handlers
        URL fileUrl = new URL("http", "example.com", 80, "/test.dat", new TestURLStreamHandler(fileContent));
        URL merkleUrl = new URL("http", "example.com", 80, "/test.dat.mrkl", new TestURLStreamHandler(merkleContent));

        // Register content for URLs
        String fileUrlString = fileUrl.toString();
        String merkleUrlString = merkleUrl.toString();
        TestURLStreamHandler.registerContent(fileUrlString, fileContent);
        TestURLStreamHandler.registerContent(merkleUrlString, merkleContent);

        // Register the merkle URL for the file URL
        TestURLStreamHandler.registerMerkleUrl(fileUrlString, merkleUrl);

        // Call the method under test
        Path localPath = tempDir.resolve("test.dat");
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);
        assertTrue(Files.exists(localPath));
        assertTrue(Files.exists(tempDir.resolve("test.dat.mrkl")));
        assertArrayEquals(fileContent, Files.readAllBytes(localPath));
    }

    /**
     * Test that the sync method correctly skips download when the local file is identical.
     */
    @Test
    void testSyncWhenFileIsIdentical() throws Exception {
        // Create test data
        byte[] fileContent = createRandomData(1024 * 1024); // 1MB
        byte[] merkleContent = createMerkleFileContent(fileContent, 16384); // 16KB chunks

        // Create local files
        Path localPath = tempDir.resolve("test.dat");
        Path localMerklePath = tempDir.resolve("test.dat.mrkl");
        Files.write(localPath, fileContent);
        Files.write(localMerklePath, merkleContent);

        // Create URLs with custom handlers
        URL fileUrl = new URL("http", "example.com", 80, "/test.dat", new TestURLStreamHandler(fileContent));
        URL merkleUrl = new URL("http", "example.com", 80, "/test.dat.mrkl", new TestURLStreamHandler(merkleContent));

        // Register content for URLs
        String fileUrlString = fileUrl.toString();
        String merkleUrlString = merkleUrl.toString();
        TestURLStreamHandler.registerContent(fileUrlString, fileContent);
        TestURLStreamHandler.registerContent(merkleUrlString, merkleContent);

        // Register the merkle URL for the file URL
        TestURLStreamHandler.registerMerkleUrl(fileUrlString, merkleUrl);

        // Record file modification times before sync
        long fileModTime = Files.getLastModifiedTime(localPath).toMillis();
        long merkleModTime = Files.getLastModifiedTime(localMerklePath).toMillis();

        // Wait a moment to ensure modification times would be different if files were updated
        Thread.sleep(100);

        // Call the method under test
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);
        assertEquals(fileModTime, Files.getLastModifiedTime(localPath).toMillis());
        assertEquals(merkleModTime, Files.getLastModifiedTime(localMerklePath).toMillis());
    }

    /**
     * Test that the sync method correctly downloads when the local file is different.
     */
    @Test
    void testSyncWhenFileIsDifferent() throws Exception {
        // Create test data
        byte[] originalContent = createRandomData(1024 * 1024); // 1MB
        byte[] modifiedContent = Arrays.copyOf(originalContent, originalContent.length);
        // Modify some bytes
        for (int i = 0; i < 1000; i++) {
            modifiedContent[i] = (byte) (modifiedContent[i] ^ 0xFF);
        }

        byte[] originalMerkleContent = createMerkleFileContent(originalContent, 16384);
        byte[] modifiedMerkleContent = createMerkleFileContent(modifiedContent, 16384);

        // Create local files with modified content
        Path localPath = tempDir.resolve("test.dat");
        Path localMerklePath = tempDir.resolve("test.dat.mrkl");
        Files.write(localPath, modifiedContent);
        Files.write(localMerklePath, modifiedMerkleContent);

        // Create URLs with custom handlers
        URL fileUrl = new URL("http", "example.com", 80, "/test.dat", new TestURLStreamHandler(originalContent));
        URL merkleUrl = new URL("http", "example.com", 80, "/test.dat.mrkl", new TestURLStreamHandler(originalMerkleContent));

        // Register content for URLs
        String fileUrlString = fileUrl.toString();
        String merkleUrlString = merkleUrl.toString();
        TestURLStreamHandler.registerContent(fileUrlString, originalContent);
        TestURLStreamHandler.registerContent(merkleUrlString, originalMerkleContent);

        // Register the merkle URL for the file URL
        TestURLStreamHandler.registerMerkleUrl(fileUrlString, merkleUrl);

        // Call the method under test
        MerkleTree tree = MerkleTree.syncFromRemote(fileUrl, localPath);

        // Verify the result
        assertNotNull(tree);
        assertArrayEquals(originalContent, Files.readAllBytes(localPath));
        assertArrayEquals(originalMerkleContent, Files.readAllBytes(localMerklePath));
    }

    /**
     * Creates random data of the specified size.
     */
    private byte[] createRandomData(int size) {
        byte[] data = new byte[size];
        new Random(42).nextBytes(data); // Use fixed seed for reproducibility
        return data;
    }

    /**
     * Creates a merkle file content for the given data and chunk size.
     */
    private byte[] createMerkleFileContent(byte[] data, int chunkSize) throws IOException {
        // Create a merkle tree from the data
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer, chunkSize, new MerkleRange(0, data.length));

        // Save the tree to a temporary file
        Path tempFile = tempDir.resolve("temp.mrkl");
        tree.save(tempFile);

        // Read the file content
        return Files.readAllBytes(tempFile);
    }

    /**
     * A custom URL stream handler that returns predefined content.
     */
    private static class TestURLStreamHandler extends URLStreamHandler {
        private final byte[] content;
        private static final Map<String, byte[]> urlContentMap = new HashMap<>();
        private static final Map<String, URL> merkleUrls = new HashMap<>();

        // Install a custom URL stream handler factory to intercept all URL creations
        static {
            try {
                URL.setURLStreamHandlerFactory(protocol -> {
                    if (protocol.equals("http") || protocol.equals("https")) {
                        return new URLStreamHandler() {
                            @Override
                            protected URLConnection openConnection(URL url) throws IOException {
                                // Check if this is a merkle URL
                                if (url.getPath().endsWith(".mrkl")) {
                                    String basePath = url.getPath().substring(0, url.getPath().length() - 5);
                                    String baseUrl = url.getProtocol() + "://" + url.getHost() +
                                                   (url.getPort() != -1 ? ":" + url.getPort() : "") + basePath;

                                    // Check if we have registered content for this merkle URL
                                    if (urlContentMap.containsKey(url.toString())) {
                                        return new TestHttpURLConnection(url, urlContentMap.get(url.toString()));
                                    }

                                    // Check if we have a registered merkle URL for the base URL
                                    if (merkleUrls.containsKey(baseUrl)) {
                                        // Use the content from the registered merkle URL
                                        URL merkleUrl = merkleUrls.get(baseUrl);
                                        if (urlContentMap.containsKey(merkleUrl.toString())) {
                                            return new TestHttpURLConnection(url, urlContentMap.get(merkleUrl.toString()));
                                        }
                                    }
                                }

                                // Check if we have registered content for this URL
                                if (urlContentMap.containsKey(url.toString())) {
                                    return new TestHttpURLConnection(url, urlContentMap.get(url.toString()));
                                }

                                // Default to a 404 response
                                return new TestHttpURLConnection(url, new byte[0]);
                            }
                        };
                    }
                    return null;
                });
            } catch (Error e) {
                // Factory already set, ignore
            }
        }

        public TestURLStreamHandler(byte[] content) {
            this.content = content;
        }

        public static void registerMerkleUrl(String fileUrl, URL merkleUrl) {
            merkleUrls.put(fileUrl, merkleUrl);
        }

        public static void registerContent(String url, byte[] content) {
            urlContentMap.put(url, content);
        }

        @Override
        protected URLConnection openConnection(URL url) {
            // Check if this is a merkle URL that we need to handle specially
            if (url.toString().endsWith(".mrkl")) {
                // Get the base URL (without .mrkl)
                String baseUrl = url.toString().substring(0, url.toString().length() - 5);

                // Check if we have registered content for this merkle URL
                if (urlContentMap.containsKey(url.toString())) {
                    return new TestHttpURLConnection(url, urlContentMap.get(url.toString()));
                }

                // Check if we have a registered merkle URL for the base URL
                if (merkleUrls.containsKey(baseUrl)) {
                    URL merkleUrl = merkleUrls.get(baseUrl);
                    // If the merkle URL has a custom handler, use it
                    if (merkleUrl.getProtocol().equals(url.getProtocol()) &&
                        merkleUrl.getHost().equals(url.getHost())) {
                        try {
                            return merkleUrl.openConnection();
                        } catch (IOException e) {
                            // Fall back to default handling
                        }
                    }
                }
            }

            // Check if we have specific content for this URL
            byte[] contentToUse = content;
            if (urlContentMap.containsKey(url.toString())) {
                contentToUse = urlContentMap.get(url.toString());
            }

            return new TestHttpURLConnection(url, contentToUse);
        }
    }

    /**
     * A custom HttpURLConnection that returns predefined content.
     */
    private static class TestHttpURLConnection extends HttpURLConnection {
        private final byte[] content;

        public TestHttpURLConnection(URL url, byte[] content) {
            super(url);
            this.content = content;
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

        @Override
        public int getResponseCode() {
            return HTTP_OK;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(content);
        }

        @Override
        public long getContentLengthLong() {
            return content.length;
        }
    }
}
