package io.nosqlbench.vectordata.transport;

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
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to ensure that ChunkedTransportIO correctly supports both HTTP and HTTPS schemes
 * through the HttpTransportProvider's @TransportScheme annotation.
 * 
 * This test verifies that the ServiceLoader mechanism properly picks up the 'https' 
 * scheme from the annotation and creates the appropriate transport client.
 */
@ExtendWith(JettyFileServerExtension.class)
public class ChunkedTransportIOSchemeTest {

    private URL baseUrl;
    private JettyFileServerFixture serverFixture;

    @BeforeEach
    public void setUp() {
        baseUrl = JettyFileServerExtension.getBaseUrl();
        serverFixture = JettyFileServerExtension.getServer();
    }

    @Test
    public void testHttpSchemeSupport() throws IOException {
        // Get the server root directory and create a test file there
        Path serverRoot = serverFixture.getRootDirectory();
        Path testDir = serverRoot.resolve("schemetest");
        Files.createDirectories(testDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path testFile = testDir.resolve("http_scheme_test_" + uniqueId + ".txt");
        String testContent = "HTTP scheme test content";
        Files.writeString(testFile, testContent);
        
        try {
            // Create HTTP URL for the test file
            String httpUrl = baseUrl.toString() + "schemetest/http_scheme_test_" + uniqueId + ".txt";
            
            // Verify the URL uses http scheme
            URL url = new URL(httpUrl);
            assertEquals("http", url.getProtocol().toLowerCase());
            
            // Create transport using ChunkedTransportIO
            try (ChunkedTransportClient transport = ChunkedTransportIO.create(httpUrl)) {
                assertNotNull(transport, "ChunkedTransportIO should create a transport for HTTP URLs");
                assertTrue(transport instanceof HttpByteRangeFetcher, 
                    "HTTP URLs should result in HttpByteRangeFetcher instances");
                assertEquals(httpUrl, transport.getSource());
            }
        } finally {
            Files.deleteIfExists(testFile);
            try {
                Files.deleteIfExists(testDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }

    @Test
    public void testHttpsSchemeSupport() throws IOException {
        // Test HTTPS scheme support even though we can't easily test against real HTTPS
        // This test verifies that the scheme is recognized and a transport is created
        String httpsUrl = "https://example.com/test.bin";
        
        try {
            // Verify the URL uses https scheme
            URL url = new URL(httpsUrl);
            assertEquals("https", url.getProtocol().toLowerCase());
            
            // Create transport using ChunkedTransportIO
            // Note: This will create the transport but not actually connect
            try (ChunkedTransportClient transport = ChunkedTransportIO.create(httpsUrl)) {
                assertNotNull(transport, "ChunkedTransportIO should create a transport for HTTPS URLs");
                assertTrue(transport instanceof HttpByteRangeFetcher, 
                    "HTTPS URLs should result in HttpByteRangeFetcher instances");
                assertEquals(httpsUrl, transport.getSource());
            }
        } catch (IOException e) {
            // Actual connection failure is expected since example.com may not support range requests
            // but we should not get "No transport provider found" error
            assertFalse(e.getMessage().contains("No transport provider found"), 
                "Should not fail due to missing transport provider for HTTPS. Error was: " + e.getMessage());
        }
    }

    @Test
    public void testServiceLoaderDiscovery() throws IOException {
        // Test that ServiceLoader can find and correctly identify the HttpTransportProvider
        // and its supported schemes
        
        java.util.ServiceLoader<io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider> loader = 
            java.util.ServiceLoader.load(io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider.class);
        
        boolean foundHttpProvider = false;
        for (io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider provider : loader) {
            if (provider instanceof HttpTransportProvider) {
                foundHttpProvider = true;
                
                // Check the annotation
                io.nosqlbench.nbdatatools.api.services.TransportScheme annotation = 
                    provider.getClass().getAnnotation(io.nosqlbench.nbdatatools.api.services.TransportScheme.class);
                
                assertNotNull(annotation, "HttpTransportProvider should have @TransportScheme annotation");
                
                String[] schemes = annotation.value();
                assertNotNull(schemes, "TransportScheme annotation should have values");
                assertTrue(schemes.length >= 2, "Should support at least 2 schemes (http and https)");
                
                boolean hasHttp = false;
                boolean hasHttps = false;
                for (String scheme : schemes) {
                    if ("http".equalsIgnoreCase(scheme)) {
                        hasHttp = true;
                    }
                    if ("https".equalsIgnoreCase(scheme)) {
                        hasHttps = true;
                    }
                }
                
                assertTrue(hasHttp, "HttpTransportProvider should support 'http' scheme");
                assertTrue(hasHttps, "HttpTransportProvider should support 'https' scheme");
                
                // Test the provider directly with both schemes
                URL httpUrl = new URL("http://example.com/test.bin");
                URL httpsUrl = new URL("https://example.com/test.bin");
                
                try (io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient httpClient = provider.getClient(httpUrl)) {
                    assertNotNull(httpClient, "Provider should create client for HTTP URL");
                } catch (UnsupportedOperationException e) {
                    fail("Should not throw UnsupportedOperationException for HTTP: " + e.getMessage());
                } catch (IOException e) {
                    // Connection failure is OK for this test
                }
                
                try (io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient httpsClient = provider.getClient(httpsUrl)) {
                    assertNotNull(httpsClient, "Provider should create client for HTTPS URL");
                } catch (UnsupportedOperationException e) {
                    fail("Should not throw UnsupportedOperationException for HTTPS: " + e.getMessage());
                } catch (IOException e) {
                    // Connection failure is OK for this test
                }
                
                break;
            }
        }
        
        assertTrue(foundHttpProvider, "ServiceLoader should find HttpTransportProvider");
    }

    @Test
    public void testHttpsSchemeDirectProvider() throws IOException {
        // Test that HttpTransportProvider directly supports HTTPS
        HttpTransportProvider provider = new HttpTransportProvider();
        
        String httpsUrl = "https://example.com/test.bin";
        URL url = new URL(httpsUrl);
        
        // This should not throw UnsupportedOperationException
        try (ChunkedTransportClient client = provider.getClient(url)) {
            assertNotNull(client, "HttpTransportProvider should create client for HTTPS URLs");
            assertTrue(client instanceof HttpByteRangeFetcher, 
                "HTTPS URLs should result in HttpByteRangeFetcher instances");
        } catch (IOException e) {
            // Connection failures are acceptable, but not scheme support failures
            assertFalse(e.getMessage().contains("only supports http"), 
                "Should not fail due to scheme not being supported");
        }
    }

    @Test
    public void testMixedCaseSchemes() throws IOException {
        // Test that scheme matching is case-insensitive
        String[] urls = {
            "HTTP://example.com/test.bin",
            "Http://example.com/test.bin", 
            "HTTPS://example.com/test.bin",
            "Https://example.com/test.bin"
        };
        
        for (String testUrl : urls) {
            try (ChunkedTransportClient transport = ChunkedTransportIO.create(testUrl)) {
                assertNotNull(transport, "Should create transport for URL: " + testUrl);
                assertTrue(transport instanceof HttpByteRangeFetcher, 
                    "Should create HttpByteRangeFetcher for URL: " + testUrl);
            } catch (IOException e) {
                // Connection failures are acceptable, but not provider lookup failures
                assertFalse(e.getMessage().contains("No transport provider found"), 
                    "Should not fail due to missing transport provider for URL: " + testUrl);
            }
        }
    }

    @Test
    public void testUnsupportedScheme() {
        // Test that unsupported schemes properly fail
        String ftpUrl = "ftp://example.com/test.bin";
        
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ChunkedTransportIO.create(ftpUrl);
        });
        
        assertTrue(exception.getMessage().contains("No transport provider found"), 
            "Should fail with 'No transport provider found' for unsupported schemes");
    }

    @Test
    public void testHttpsUrlDirectTest() {
        // Simple test demonstrating HTTPS URL support
        String httpsUrl = "https://httpbin.org/get";
        
        // This should NOT throw "No transport provider found"
        try (ChunkedTransportClient client = ChunkedTransportIO.create(httpsUrl)) {
            assertNotNull(client, "Should create client for HTTPS URL");
            assertEquals(httpsUrl, client.getSource());
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("No transport provider found")) {
                fail("ChunkedTransportIO failed to find provider for HTTPS URL - this indicates the annotation issue");
            }
            // Other IllegalArgumentExceptions might be valid
        } catch (IOException e) {
            // Network issues are fine for this test - we just want to verify the provider is found
        }
    }

    @Test 
    public void testHttpToHttpsUrlUpgrade() throws IOException {
        // Verify that HTTP URLs can be manually converted to HTTPS and still work
        Path serverRoot = serverFixture.getRootDirectory();
        Path testDir = serverRoot.resolve("upgradetest");
        Files.createDirectories(testDir);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path testFile = testDir.resolve("upgrade_test_" + uniqueId + ".txt");
        String testContent = "URL upgrade test content";
        Files.writeString(testFile, testContent);
        
        try {
            // Start with HTTP URL
            String httpUrl = baseUrl.toString() + "upgradetest/upgrade_test_" + uniqueId + ".txt";
            
            // Convert to HTTPS URL (just for scheme testing, won't actually work with test server)
            String httpsUrl = httpUrl.replace("http://", "https://");
            
            // Both should create transports (though HTTPS won't connect to our test server)
            try (ChunkedTransportClient httpTransport = ChunkedTransportIO.create(httpUrl)) {
                assertNotNull(httpTransport);
            }
            
            try (ChunkedTransportClient httpsTransport = ChunkedTransportIO.create(httpsUrl)) {
                assertNotNull(httpsTransport, "HTTPS URL should create transport even if connection fails");
            } catch (IOException e) {
                // Connection failure is expected, but provider should be found
                assertFalse(e.getMessage().contains("No transport provider found"));
            }
            
        } finally {
            Files.deleteIfExists(testFile);
            try {
                Files.deleteIfExists(testDir);
            } catch (java.nio.file.DirectoryNotEmptyException e) {
                // Ignore if directory not empty
            }
        }
    }
}