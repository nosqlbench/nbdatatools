package io.nosqlbench.vectordata.downloader.testserver;

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

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.http.io.HttpRequestHandler;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/// A test fixture that starts a simple web server to host datasets and catalogs.
///
/// This fixture starts an HTTP server on a random available port and serves files
/// from the test resources directory. It provides methods to get the server URL
/// for use in tests and automatically cleans up resources when the test is done.
///
/// Example usage:
/// ```java
/// try (TestWebServerFixture server = new TestWebServerFixture()) {
///     server.start();
///     URL baseUrl = server.getBaseUrl();
///     // Use baseUrl in your tests
/// }
/// ```
public class TestWebServerFixture implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(TestWebServerFixture.class);

    private HttpServer server;
    private int port;
    private final Path resourcesRoot;
    private final Map<Path, FileTime> fileTimestamps = new HashMap<>();

    /// Creates a new TestWebServerFixture with the default resources directory.
    public TestWebServerFixture() {
        this(Paths.get("src/test/resources/testserver"));
    }

    /// Creates a new TestWebServerFixture with the specified resources directory.
    ///
    /// @param resourcesRoot The root directory containing the resources to serve
    public TestWebServerFixture(Path resourcesRoot) {
        logger.debug("resourcesRoot: {}", resourcesRoot);
        this.resourcesRoot = resourcesRoot;

        // Ensure the resources directory exists
        if (!Files.exists(resourcesRoot)) {
            throw new UncheckedIOException(new IOException("Resources directory does not exist: " + resourcesRoot));
        }

        // Install a security check to prevent modifications to the testserver directory
        installReadOnlyCheck(resourcesRoot);
    }

    /// Takes a snapshot of file timestamps in the testserver directory.
    ///
    /// This method records the last modified time of all files in the directory
    /// so we can detect if any files are modified during the test.
    ///
    /// @param directory The directory to snapshot
    private void installReadOnlyCheck(Path directory) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    fileTimestamps.put(file, Files.getLastModifiedTime(file));
                    return FileVisitResult.CONTINUE;
                }
            });
            logger.debug("Took timestamp snapshot of {} files in {}", fileTimestamps.size(), directory);
        } catch (IOException e) {
            logger.warn("Failed to take file timestamp snapshot: {}", e.getMessage());
        }
    }

    /// Starts the web server on a random available port.
    ///
    /// @throws IOException If the server cannot be started
    public void start() throws IOException {
        // Find an available port
        this.port = findAvailablePort();

        // Create and configure the server
        server = ServerBootstrap.bootstrap()
                .setListenerPort(port)
                .register("*", new FileHandler(resourcesRoot))
                .create();

        // Start the server
        server.start();
        logger.info("Test web server started on port {}", port);
    }

    /// Gets the base URL of the server.
    ///
    /// @return The base URL of the server
    public URL getBaseUrl() {
        try {
            return new URL("http://127.0.0.1:" + port + "/");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create server URL", e);
        }
    }

    /// Stops the server and releases resources.
    /// Also verifies that no files in the testserver directory have been modified.
    @Override
    public void close() {
        if (server != null) {
            server.close(CloseMode.GRACEFUL);
            logger.info("Test web server stopped");
        }

        // Check if any files in the testserver directory have been modified
        checkForModifiedFiles();
    }

    /// Checks if any files in the testserver directory have been modified.
    /// Throws an exception if any files have been modified.
    private void checkForModifiedFiles() {
        if (fileTimestamps.isEmpty()) {
            return; // No files were recorded, nothing to check
        }

        try {
            for (Map.Entry<Path, FileTime> entry : fileTimestamps.entrySet()) {
                Path file = entry.getKey();
                FileTime originalTime = entry.getValue();

                if (Files.exists(file)) {
                    FileTime currentTime = Files.getLastModifiedTime(file);
                    if (!currentTime.equals(originalTime)) {
                        throw new IllegalStateException(
                            "Unit tests are not allowed to modify files in the testserver directory. " +
                            "File was modified: " + file);
                    }
                } else {
                    throw new IllegalStateException(
                        "Unit tests are not allowed to delete files in the testserver directory. " +
                        "File was deleted: " + file);
                }
            }

            // Also check for new files
            final Path directory = resourcesRoot;
            final Set<Path> currentFiles = new HashSet<>();

            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    currentFiles.add(file);
                    return FileVisitResult.CONTINUE;
                }
            });

            // Check if there are any new files that weren't in the original snapshot
            for (Path currentFile : currentFiles) {
                if (!fileTimestamps.containsKey(currentFile)) {
                    throw new IllegalStateException(
                        "Unit tests are not allowed to create new files in the testserver directory. " +
                        "New file was created: " + currentFile);
                }
            }

        } catch (IOException e) {
            logger.warn("Failed to check for modified files: {}", e.getMessage());
        }
    }

    /// Finds an available port to use for the server.
    ///
    /// @return An available port number
    private int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available port", e);
        }
    }

    /// HTTP handler that serves files from the resources directory.
    protected static class FileHandler implements HttpRequestHandler {
        private final Path resourcesRoot;

        public FileHandler(Path resourcesRoot) {
            this.resourcesRoot = resourcesRoot;
        }



        @Override
        public void handle(ClassicHttpRequest request, ClassicHttpResponse response, HttpContext context) throws HttpException, IOException {
            String path = request.getPath();
            String method = request.getMethod();

            // Remove leading slash and normalize path
            if (path.startsWith("/")) {
                path = path.substring(1);
            }

            Path filePath = resourcesRoot.resolve(path);

            // Return 404 for directory URLs (ending with /) but only if the path doesn't exist as a file
            if ((path.endsWith("/") || path.isEmpty()) && !(Files.exists(filePath) && Files.isRegularFile(filePath))) {
                response.setCode(HttpStatus.SC_NOT_FOUND);
                if (!"HEAD".equalsIgnoreCase(method)) {
                    response.setEntity(new StringEntity("Directory access not allowed: " + path + ". URLs must resolve to files."));
                }
                return;
            }

            // Check if the file exists
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                // Handle range requests for partial content
                Header rangeHeader = request.getHeader("Range");
                if (rangeHeader != null) {
                    handleRangeRequest(response, filePath, rangeHeader.getValue());
                } else {
                    // For HEAD requests, set headers but don't include body
                    if ("HEAD".equalsIgnoreCase(method)) {
                        handleHeadRequest(response, filePath);
                    } else {
                        // Serve the entire file for GET and other methods
                        serveFile(response, filePath);
                    }
                }
            } else {
                // File not found
                response.setCode(HttpStatus.SC_NOT_FOUND);
                if (!"HEAD".equalsIgnoreCase(method)) {
                    response.setEntity(new StringEntity("File not found: " + path));
                }
            }
        }

        /// Serves the entire file.
        ///
        /// @param response The HTTP response
        /// @param filePath The path to the file to serve
        private void serveFile(ClassicHttpResponse response, Path filePath) throws IOException {
            response.setCode(HttpStatus.SC_OK);
            response.setEntity(new FileEntity(filePath.toFile(), ContentType.APPLICATION_OCTET_STREAM));
        }

        /// Handles a HEAD request by setting headers without a response body.
        ///
        /// @param response The HTTP response
        /// @param filePath The path to the file
        private void handleHeadRequest(ClassicHttpResponse response, Path filePath) throws IOException {
            response.setCode(HttpStatus.SC_OK);

            // Set Content-Type header
            response.setHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.toString());

            // Set Content-Length header
            long fileSize = Files.size(filePath);
            response.setHeader("Content-Length", String.valueOf(fileSize));

            // Set Accept-Ranges header
            response.setHeader("Accept-Ranges", "bytes");

            // No entity (body) is set for HEAD requests
        }

        /// Handles a range request for partial content.
        ///
        /// @param response The HTTP response
        /// @param filePath The path to the file to serve
        /// @param rangeHeader The Range header value
        private void handleRangeRequest(ClassicHttpResponse response, Path filePath, String rangeHeader) throws IOException {
            // Parse the range header
            if (!rangeHeader.startsWith("bytes=")) {
                response.setCode(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                response.setEntity(new StringEntity("Invalid range header format. Expected 'bytes=start-end' format."));
                return;
            }

            long fileSize = Files.size(filePath);
            String[] ranges = rangeHeader.substring(6).split("-");

            long start = 0;
            long end = fileSize - 1;

            if (ranges.length > 0 && !ranges[0].isEmpty()) {
                start = Long.parseLong(ranges[0]);
            }

            if (ranges.length > 1 && !ranges[1].isEmpty()) {
                end = Long.parseLong(ranges[1]);
            }

            // Validate the range
            if (start >= fileSize || end >= fileSize || start > end) {
                response.setCode(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                response.setEntity(new StringEntity("Invalid range values. Requested range (" + start + "-" + end + 
                    ") is outside the valid range for the file (0-" + (fileSize - 1) + ")."));
                return;
            }

            // Set the response headers
            response.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + fileSize);
            response.setHeader("Accept-Ranges", "bytes");
            response.setCode(HttpStatus.SC_PARTIAL_CONTENT);

            // Calculate content length
            long contentLength = end - start + 1;

            // Read the requested range into a byte array
            byte[] buffer = new byte[(int)contentLength];
            try (InputStream in = Files.newInputStream(filePath)) {
                long skipped = in.skip(start);
                if (skipped < start) {
                    // Handle case where skip didn't skip enough bytes
                    long remaining = start - skipped;
                    while (remaining > 0) {
                        long skippedMore = in.skip(remaining);
                        if (skippedMore <= 0) break;
                        remaining -= skippedMore;
                    }
                }

                // Read the content into the buffer
                int bytesRead = 0;
                int totalBytesRead = 0;
                int bytesToRead = (int)contentLength;

                while (totalBytesRead < bytesToRead && 
                       (bytesRead = in.read(buffer, totalBytesRead, bytesToRead - totalBytesRead)) != -1) {
                    totalBytesRead += bytesRead;
                }

                // If we couldn't read all the bytes, adjust the content length
                if (totalBytesRead < bytesToRead) {
                    contentLength = totalBytesRead;
                    response.setHeader("Content-Range", "bytes " + start + "-" + (start + totalBytesRead - 1) + "/" + fileSize);
                }
            }

            // Create an entity from the buffer
            response.setEntity(new InputStreamEntity(
                new java.io.ByteArrayInputStream(buffer, 0, (int)contentLength), 
                contentLength, 
                ContentType.APPLICATION_OCTET_STREAM
            ));
        }
    }
}
