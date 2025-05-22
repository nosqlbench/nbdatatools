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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

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
    
    /// Creates a new TestWebServerFixture with the default resources directory.
    public TestWebServerFixture() {
        this(Paths.get("src/test/resources/testserver"));
    }
    
    /// Creates a new TestWebServerFixture with the specified resources directory.
    ///
    /// @param resourcesRoot The root directory containing the resources to serve
    public TestWebServerFixture(Path resourcesRoot) {
        this.resourcesRoot = resourcesRoot;
    }
    
    /// Starts the web server on a random available port.
    ///
    /// @throws IOException If the server cannot be started
    public void start() throws IOException {
        // Find an available port
        this.port = findAvailablePort();
        
        // Create and configure the server
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new FileHandler(resourcesRoot));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        
        // Start the server
        server.start();
        logger.info("Test web server started on port {}", port);
    }
    
    /// Gets the base URL of the server.
    ///
    /// @return The base URL of the server
    public URL getBaseUrl() {
        try {
            return new URL("http://localhost:" + port + "/");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create server URL", e);
        }
    }
    
    /// Stops the server and releases resources.
    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
            logger.info("Test web server stopped");
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
    private static class FileHandler implements HttpHandler {
        private final Path resourcesRoot;
        
        public FileHandler(Path resourcesRoot) {
            this.resourcesRoot = resourcesRoot;
        }
        
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            
            // Remove leading slash and normalize path
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            
            // Default to catalog.json if path is empty or ends with /
            if (path.isEmpty() || path.endsWith("/")) {
                path = path + "catalog.json";
            }
            
            Path filePath = resourcesRoot.resolve(path);
            
            // Check if the file exists
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                // Handle range requests for partial content
                String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                if (rangeHeader != null) {
                    handleRangeRequest(exchange, filePath, rangeHeader);
                } else {
                    // Serve the entire file
                    serveFile(exchange, filePath);
                }
            } else {
                // File not found
                exchange.sendResponseHeaders(404, 0);
                exchange.getResponseBody().close();
            }
        }
        
        /// Serves the entire file.
        ///
        /// @param exchange The HTTP exchange
        /// @param filePath The path to the file to serve
        private void serveFile(HttpExchange exchange, Path filePath) throws IOException {
            try (InputStream in = Files.newInputStream(filePath)) {
                exchange.sendResponseHeaders(200, Files.size(filePath));
                try (OutputStream out = exchange.getResponseBody()) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
            }
        }
        
        /// Handles a range request for partial content.
        ///
        /// @param exchange The HTTP exchange
        /// @param filePath The path to the file to serve
        /// @param rangeHeader The Range header value
        private void handleRangeRequest(HttpExchange exchange, Path filePath, String rangeHeader) throws IOException {
            // Parse the range header
            if (!rangeHeader.startsWith("bytes=")) {
                exchange.sendResponseHeaders(416, 0);
                exchange.getResponseBody().close();
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
                exchange.sendResponseHeaders(416, 0);
                exchange.getResponseBody().close();
                return;
            }
            
            // Set the response headers
            exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + fileSize);
            exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
            
            // Send the partial content
            long contentLength = end - start + 1;
            exchange.sendResponseHeaders(206, contentLength);
            
            try (InputStream in = Files.newInputStream(filePath);
                 OutputStream out = exchange.getResponseBody()) {
                in.skip(start);
                
                byte[] buffer = new byte[8192];
                long remaining = contentLength;
                int bytesRead;
                
                while (remaining > 0 && (bytesRead = in.read(buffer, 0, (int) Math.min(buffer.length, remaining))) != -1) {
                    out.write(buffer, 0, bytesRead);
                    remaining -= bytesRead;
                }
            }
        }
    }
}