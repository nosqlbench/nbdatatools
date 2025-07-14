package io.nosqlbench.jetty.testserver;

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
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.atomic.AtomicInteger;

////*
///  A JUnit Jupiter extension that manages a JettyFileServerFixture at the module level.
///
///  This extension starts a single JettyFileServerFixture before any tests in the module run
///  and stops it after all tests in the module have completed. It uses a static initialization
///  mechanism to ensure the server is started when first accessed and stopped when the JVM exits.
///
///  The extension is applied at the module level through the VectorDataTestSuite class,
///  which is configured as a JUnit Platform Suite that includes all tests in the vectordata
/// module.
/// This means that all tests in the module have access to the test web server fixture without
/// requiring any additional annotations.
///
/// Example usage:
///
/// ```java
/// @ExtendWith(JettyFileServerExtension.class)
/// public class MyTest {
///     // Test methods
/// }
/// ```
///
public class JettyFileServerExtension implements BeforeAllCallback, AfterAllCallback {
    private static final Logger logger = LogManager.getLogger(JettyFileServerExtension.class);
    private static final AtomicInteger referenceCount = new AtomicInteger(0);
    private static JettyFileServerFixture server;
    private static URL baseUrl;

    // Default paths can be overridden by setting system properties
    private static final String RESOURCES_ROOT_PROPERTY = "jetty.test.resources.root";
    private static final String DEFAULT_RESOURCES_PATH = "src/test/resources/testserver";

    public static final Path DEFAULT_RESOURCES_ROOT;
    public static final Path TEMP_RESOURCES_ROOT;

    private static final Object lock = new Object();

    // Static initializer to configure paths
    static {
        String resourcesPath = System.getProperty(RESOURCES_ROOT_PROPERTY, DEFAULT_RESOURCES_PATH);
        DEFAULT_RESOURCES_ROOT = Paths.get(resourcesPath).toAbsolutePath();
        TEMP_RESOURCES_ROOT = DEFAULT_RESOURCES_ROOT.resolve("temp");
    }

    /**
     * Initializes and starts the server if not already started.
     * This method is thread-safe and idempotent.
     */
    public static void initialize() {
        synchronized (lock) {
            if (server == null) {
                try {
                    // Create temp directory if it doesn't exist
                    try {
                        Files.createDirectories(
                            TEMP_RESOURCES_ROOT,
                            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"))
                        );
                    } catch (UnsupportedOperationException e) {
                        // Fall back to simple directory creation on systems that don't support POSIX permissions
                        Files.createDirectories(TEMP_RESOURCES_ROOT);
                    }

                    logger.info("Starting Jetty test web server for the module");
                    server = new JettyFileServerFixture(DEFAULT_RESOURCES_ROOT);
                    server.setTempDirectory(TEMP_RESOURCES_ROOT);
                    server.start();
                    baseUrl = server.getBaseUrl();
                    logger.info("Jetty test web server started at {}", baseUrl);

                    // Add shutdown hook to stop the server when the JVM exits
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        if (server != null) {
                            logger.info("Stopping Jetty test web server for the module (shutdown hook)");
                            server.close();
                            server = null;
                            baseUrl = null;
                            logger.info("Jetty test web server stopped");
                        }
                    }));
                } catch (IOException e) {
                    logger.error("Failed to start Jetty test web server", e);
                    throw new RuntimeException("Failed to start Jetty test web server", e);
                }
            }
        }
    }

    /**
     * Gets the base URL of the test web server.
     * @return The base URL of the test web server
     */
    public static URL getBaseUrl() {
        initialize();
        return baseUrl;
    }

    /**
     * Gets the JettyFileServerFixture instance.
     * @return The JettyFileServerFixture instance
     */
    public static JettyFileServerFixture getServer() {
        initialize();
        return server;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Initialize server when extension is used
        initialize();
        logger.debug("JettyFileServerExtension beforeAll called for {}", context.getDisplayName());
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Server will be stopped by shutdown hook
        // Just log that the extension is being used
        logger.debug("JettyFileServerExtension afterAll called for {}", context.getDisplayName());
    }
}