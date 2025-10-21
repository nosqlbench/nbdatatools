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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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


/**
 * A test fixture that starts a Jetty web server to host datasets and catalogs.
 * <p>
 * This fixture starts an HTTP server on a random available port and serves files
 * from the test resources directory. It provides methods to get the server URL
 * for use in tests and automatically cleans up resources when the test is done.
 * <p>
 * Example usage:
 * ```java
 * try (JettyFileServerFixture server = new JettyFileServerFixture()) {
 *     server.start();
 *     URL baseUrl = server.getBaseUrl();
 *     // Use baseUrl in your tests
 * }
 * ```
 */
public class JettyFileServerFixture implements AutoCloseable {
    static {
        ensureHostnameResolution();
        if (System.getProperty("log4j2.hostname") == null) {
            System.setProperty("log4j2.hostname", "localhost");
        }
        if (System.getProperty("LOG4J_HOSTNAME") == null) {
            System.setProperty("LOG4J_HOSTNAME", "localhost");
        }
        if (System.getProperty("log4j2.StatusLogger.level") == null) {
            System.setProperty("log4j2.StatusLogger.level", "FATAL");
        }
    }

    private static Logger logger() {
        return LazyLoggerHolder.LOGGER;
    }

    private static class LazyLoggerHolder {
        private static final Logger LOGGER = LogManager.getLogger(JettyFileServerFixture.class);
    }

    private static void ensureHostnameResolution() {
        if (System.getProperty("jdk.net.hosts.file") != null) {
            return;
        }
        try {
            Path hostsFile = Files.createTempFile("nbtest-hosts", ".cfg");
            String content = "127.0.0.1 localhost\n127.0.0.1 testdata\n";
            Files.writeString(hostsFile, content, StandardCharsets.UTF_8);
            hostsFile.toFile().deleteOnExit();
            System.setProperty("jdk.net.hosts.file", hostsFile.toString());
        } catch (IOException ignored) {
            // can't log yet
        }
    }

    private Server server;
    private int port;
    private final Path resourcesRoot;
    private final Map<Path, FileTime> fileTimestamps = new HashMap<>();
    private Path tempDirectory = null;

    /**
     * Creates a new JettyFileServerFixture with the default resources directory.
     */
    public JettyFileServerFixture() {
        this(Paths.get("src/test/resources/testserver"));
    }

    /**
     * Creates a new JettyFileServerFixture with the specified resources directory.
     *
     * @param resourcesRoot The root directory containing the resources to serve
     */
    public JettyFileServerFixture(Path resourcesRoot) {
        logger().debug("resourcesRoot: {}", resourcesRoot);
        this.resourcesRoot = resourcesRoot;

        // Ensure the resources directory exists
        if (!Files.exists(resourcesRoot)) {
            throw new UncheckedIOException(new IOException("Resources directory does not exist: " + resourcesRoot));
        }

        // Install a security check to prevent modifications to the testserver directory
        installReadOnlyCheck(resourcesRoot);
    }

    /**
     * Sets the temporary directory for this server instance.
     * Files in this directory and its subdirectories will be excluded from modification checks.
     *
     * @param tempDirectory The temporary directory path
     */
    public void setTempDirectory(Path tempDirectory) {
        this.tempDirectory = tempDirectory;
    }

    /**
     * Takes a snapshot of file timestamps in the testserver directory.
     * <p>
     * This method records the last modified time of all files in the directory
     * so we can detect if any files are modified during the test.
     * Files in the temp directory are excluded from the snapshot.
     *
     * @param directory The directory to snapshot
     */
    private void installReadOnlyCheck(Path directory) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    // Skip the temp directory and its subdirectories
                    if (isInTempDirectory(dir)) {
                        logger().debug("Skipping temp directory: {}", dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // Skip files in the temp directory
                    if (isInTempDirectory(file)) {
                        logger().debug("Skipping file in temp directory: {}", file);
                        return FileVisitResult.CONTINUE;
                    }

                    fileTimestamps.put(file, Files.getLastModifiedTime(file));
                    return FileVisitResult.CONTINUE;
                }
            });
            logger().debug("Took timestamp snapshot of {} files in {} (excluding temp directory)", fileTimestamps.size(), directory);
        } catch (IOException e) {
            logger().warn("Failed to take file timestamp snapshot: {}", e.getMessage());
        }
    }

    /**
     * Starts the web server on a random available port.
     *
     * @throws IOException If the server cannot be started
     */
    public void start() throws IOException {
        // Find an available port
        this.port = findAvailablePort();

        // Create the server
        server = new Server();

        // Add HTTP connector
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        connector.setPort(port);
        server.addConnector(connector);

        // Create a servlet context handler
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(resourcesRoot.toAbsolutePath().toString());
        server.setHandler(context);

        // Add the default servlet for serving static files
        // Jetty's DefaultServlet already has built-in support for range requests
        ServletHolder defaultServlet = new ServletHolder("default", DefaultServlet.class);
        defaultServlet.setInitParameter("dirAllowed", "true");
        defaultServlet.setInitParameter("pathInfoOnly", "false");
        defaultServlet.setInitParameter("welcomeServlets", "false");
        defaultServlet.setInitParameter("redirectWelcome", "false");
        defaultServlet.setInitParameter("precompressed", "false");
        defaultServlet.setInitParameter("acceptRanges", "true");  // Enable range request support
        defaultServlet.setInitParameter("etags", "true");         // Enable ETag support

        // Additional parameters to ensure range requests work correctly
        defaultServlet.setInitParameter("cacheControl", "max-age=3600,public");

        context.addServlet(defaultServlet, "/");

        try {
            // Start the server
            server.start();
            logger().info("Jetty test web server started on port {} serving files from {}", port, resourcesRoot);
        } catch (Exception e) {
            throw new IOException("Failed to start Jetty server", e);
        }
    }

    /**
     * Gets the base URL of the server.
     *
     * @return The base URL of the server
     */
    public URL getBaseUrl() {
        try {
            return new URL("http://127.0.0.1:" + port + "/");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create server URL", e);
        }
    }

    /**
     * Gets the root directory being served by this server.
     *
     * @return The root directory path
     */
    public Path getRootDirectory() {
        return resourcesRoot;
    }

    /**
     * Stops the server and releases resources.
     * Also verifies that no files in the testserver directory have been modified.
     */
    @Override
    public void close() {
        if (server != null) {
            try {
                server.stop();
                logger().info("Jetty test web server stopped");
            } catch (Exception e) {
                logger().error("Error stopping Jetty server", e);
            }
        }

        // Check if any files in the testserver directory have been modified
        checkForModifiedFiles();
    }

    /**
     * Checks if any files in the testserver directory have been modified.
     * Throws an exception if any files have been modified.
     * Ignores changes to files in directories under the base content directory under temp.
     */
    private void checkForModifiedFiles() {
        if (fileTimestamps.isEmpty()) {
            return; // No files were recorded, nothing to check
        }

        try {
            for (Map.Entry<Path, FileTime> entry : fileTimestamps.entrySet()) {
                Path file = entry.getKey();
                FileTime originalTime = entry.getValue();

                // Skip files in directories under the temp directory
                if (isInTempDirectory(file)) {
                    continue;
                }

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
                // Skip files in directories under the temp directory
                if (isInTempDirectory(currentFile)) {
                    continue;
                }

                if (!fileTimestamps.containsKey(currentFile)) {
                    throw new IllegalStateException(
                        "Unit tests are not allowed to create new files in the testserver directory. " +
                        "New file was created: " + currentFile);
                }
            }

        } catch (IOException e) {
            logger().warn("Failed to check for modified files: {}", e.getMessage());
        }
    }

    /**
     * Checks if a file is in a directory under the temp directory.
     *
     * @param file The file to check
     * @return True if the file is in a directory under the temp directory, false otherwise
     */
    private boolean isInTempDirectory(Path file) {
        if (tempDirectory == null) {
            return false;
        }

        // Check if the file is in the temp directory or a subdirectory of it
        return file.startsWith(tempDirectory);
    }

    /**
     * Finds an available port to use for the server.
     *
     * @return An available port number
     */
    private int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available port", e);
        }
    }

    // Note: We're using Jetty's built-in DefaultServlet which already has support for range requests.
    // This simplifies our implementation and avoids the need for a custom servlet.
}
