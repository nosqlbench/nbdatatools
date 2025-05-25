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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.*;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

/// A JUnit Jupiter extension that manages a TestWebServerFixture at the module level.
///
/// This extension starts a single TestWebServerFixture before any tests in the module run
/// and stops it after all tests in the module have completed. It uses a static initialization
/// mechanism to ensure the server is started when first accessed and stopped when the JVM exits.
///
/// The extension is applied at the module level through the VectorDataTestSuite class,
/// which is configured as a JUnit Platform Suite that includes all tests in the vectordata module.
/// This means that all tests in the module have access to the test web server fixture without
/// requiring the @EnableTestWebServer annotation.
///
/// Example usage:
/// ```java
/// public class MyTest {
///     @Test
///     public void testSomething() {
///         URL baseUrl = TestWebServerExtension.getBaseUrl();
///         // Use baseUrl in your test
///     }
/// }
/// ```
public class TestWebServerExtension implements BeforeAllCallback, AfterAllCallback {
    private static final Logger logger = LogManager.getLogger(TestWebServerExtension.class);
    private static final AtomicInteger referenceCount = new AtomicInteger(0);
    private static TestWebServerFixture server;
    private static URL baseUrl;
    private static final Path DEFAULT_RESOURCES_ROOT = Paths.get("src/test/resources/testserver");
    private static final Object lock = new Object();

    // Static initializer to start the server when the class is loaded
    static {
        try {
            synchronized (lock) {
                if (server == null) {
                    logger.info("Starting test web server for the module (static initialization)");
                    server = new TestWebServerFixture(DEFAULT_RESOURCES_ROOT);
                    server.start();
                    baseUrl = server.getBaseUrl();
                    logger.info("Test web server started at {}", baseUrl);

                    // Add shutdown hook to stop the server when the JVM exits
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        if (server != null) {
                            logger.info("Stopping test web server for the module (shutdown hook)");
                            server.close();
                            server = null;
                            baseUrl = null;
                            logger.info("Test web server stopped");
                        }
                    }));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to start test web server", e);
            throw new RuntimeException("Failed to start test web server", e);
        }
    }

    /// Gets the base URL of the test web server.
    ///
    /// @return The base URL of the test web server
    public static URL getBaseUrl() {
        // Server is started by static initializer, so baseUrl should never be null
        return baseUrl;
    }

    /// Gets the TestWebServerFixture instance.
    ///
    /// @return The TestWebServerFixture instance
    public static TestWebServerFixture getServer() {
        // Server is started by static initializer, so server should never be null
        return server;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Server is already started by static initializer
        // Just log that the extension is being used
        logger.debug("TestWebServerExtension beforeAll called for {}", context.getDisplayName());
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Server will be stopped by shutdown hook
        // Just log that the extension is being used
        logger.debug("TestWebServerExtension afterAll called for {}", context.getDisplayName());
    }
}
