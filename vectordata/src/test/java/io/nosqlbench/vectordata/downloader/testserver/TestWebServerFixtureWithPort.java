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

import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.io.CloseMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/// A test fixture that starts a simple web server to host datasets and catalogs on a specified port.
///
/// This fixture extends TestWebServerFixture to allow specifying a custom port.
/// It provides methods to get the server URL for use in tests and automatically 
/// cleans up resources when the test is done.
///
/// Example usage:
/// ```java
/// try (TestWebServerFixtureWithPort server = new TestWebServerFixtureWithPort(8080)) {
///     server.start();
///     URL baseUrl = server.getBaseUrl();
///     // Use baseUrl in your tests
/// }
/// ```
public class TestWebServerFixtureWithPort extends TestWebServerFixture {
    private static final Logger logger = LogManager.getLogger(TestWebServerFixtureWithPort.class);

    private HttpServer server;
    private final int port;
    private final Path resourcesRoot;

    /// Creates a new TestWebServerFixtureWithPort with the default resources directory and specified port.
    ///
    /// @param port The port to use for the server
    public TestWebServerFixtureWithPort(int port) {
        this(Paths.get("src/test/resources/testserver"), port);
    }

    /// Creates a new TestWebServerFixtureWithPort with the specified resources directory and port.
    ///
    /// @param resourcesRoot The root directory containing the resources to serve
    /// @param port The port to use for the server
    public TestWebServerFixtureWithPort(Path resourcesRoot, int port) {
        super(resourcesRoot);
        this.resourcesRoot = resourcesRoot;
        this.port = port;
    }

    /// Starts the web server on the specified port.
    ///
    /// @throws IOException If the server cannot be started
    @Override
    public void start() throws IOException {
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
    @Override
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
            server.close(CloseMode.GRACEFUL);
            logger.info("Test web server stopped");
        }
    }
}