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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/// Tests that verify the TestWebServerFixture behaves like a normal web server.
///
/// This test verifies that the web server does not have special case behavior
/// for catalog.json when accessing directories or the root path.
public class TestWebServerNormalBehaviorTest {

    @Test
    public void testDirectoryAccessDoesNotDefaultToCatalog() throws IOException {
        // Create a standalone server for this test
        Path resourcesRoot = Paths.get("src/test/resources/testserver").toAbsolutePath();
        try (TestWebServerFixture server = new TestWebServerFixture(resourcesRoot)) {
            server.start();
            URL baseUrl = server.getBaseUrl();

            // Test accessing a directory (URL ending with /)
            URL directoryUrl = new URL(baseUrl, "testxvec/");
            HttpURLConnection connection = (HttpURLConnection) directoryUrl.openConnection();
            try {
                // Check the HTTP status code - should be 404 since we removed the special case
                int statusCode = connection.getResponseCode();
                assertEquals(404, statusCode, "HTTP status code should be 404 for directory access");
            } finally {
                connection.disconnect();
            }

            // Test accessing the root path
            URL rootUrl = new URL(baseUrl.toString());
            connection = (HttpURLConnection) rootUrl.openConnection();
            try {
                // Check the HTTP status code - should be 404 since we removed the special case
                int statusCode = connection.getResponseCode();
                assertEquals(404, statusCode, "HTTP status code should be 404 for root path access");
            } finally {
                connection.disconnect();
            }

            // Verify that explicit catalog.json access still works
            URL catalogUrl = new URL(baseUrl, "catalog.json");
            connection = (HttpURLConnection) catalogUrl.openConnection();
            try {
                // Check the HTTP status code
                int statusCode = connection.getResponseCode();
                assertEquals(200, statusCode, "HTTP status code should be 200 for explicit catalog.json access");

                // Verify the content
                try (java.io.InputStream in = connection.getInputStream()) {
                    byte[] data = in.readAllBytes();
                    assertTrue(data.length > 0, "Catalog content should not be empty");
                    String content = new String(data);
                    assertTrue(content.contains("testxvec"), "Catalog should contain testxvec dataset");
                }
            } finally {
                connection.disconnect();
            }
        }
    }
}
