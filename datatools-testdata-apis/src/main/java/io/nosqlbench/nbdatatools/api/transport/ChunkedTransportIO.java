package io.nosqlbench.nbdatatools.api.transport;

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

import io.nosqlbench.nbdatatools.api.services.TransportScheme;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;

/// Factory for creating appropriate ChunkedTransportClient instances based on URL scheme.
/// 
/// This factory uses ServiceLoader to discover available transport providers and selects
/// the appropriate implementation based on the @TransportScheme annotation. The factory
/// does not instantiate providers until a matching scheme is found.
/// 
/// The factory handles URL parsing, validation, and delegates to the appropriate provider
/// for creating transport clients.
/// 
/// Example usage:
/// ```java
/// // HTTP source
/// ChunkedTransportClient httpClient = ChunkedTransportIO.create("https://example.com/data.bin");
/// 
/// // Local file
/// ChunkedTransportClient fileClient = ChunkedTransportIO.create("file:///path/to/data.bin");
/// ChunkedTransportClient fileClient2 = ChunkedTransportIO.create("/path/to/data.bin");
/// ```
public class ChunkedTransportIO {

    /// Creates an appropriate ChunkedTransportClient instance for the given URL.
    /// 
    /// The method uses ServiceLoader to find transport providers and selects the first
    /// one that supports the URL's scheme based on the @TransportScheme annotation.
    /// 
    /// @param url The URL or path to the data source
    /// @return A ChunkedTransportClient instance appropriate for the URL type
    /// @throws IllegalArgumentException if the URL is null, empty, or has an unsupported scheme
    /// @throws IOException if the URL cannot be parsed or the target cannot be accessed
    public static ChunkedTransportClient create(String url) throws IOException {
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException("URL cannot be null or empty");
        }

        url = url.trim();
        String scheme;
        
        try {
            // Try to parse as URI to extract scheme
            URI uri = new URI(url);
            scheme = uri.getScheme();
            
            if (scheme == null) {
                // No scheme detected, treat as local file path
                scheme = "file";
            }
        } catch (URISyntaxException e) {
            // If URI parsing fails, treat as local file path
            scheme = "file";
        }
        
        // Convert string URL to URL object
        URL urlObj;
        try {
            if (scheme == null || "file".equalsIgnoreCase(scheme)) {
                // Handle file paths
                if (url.startsWith("file://")) {
                    urlObj = new URL(url);
                } else {
                    // Convert local path to file URL
                    Path path = Paths.get(url).toAbsolutePath();
                    urlObj = path.toUri().toURL();
                }
            } else {
                // HTTP/HTTPS URLs
                urlObj = new URL(url);
            }
        } catch (MalformedURLException e) {
            throw new IOException("Invalid URL: " + url, e);
        }
        
        // Use ServiceLoader to find appropriate transport provider
        ServiceLoader<ChunkedTransportProvider> loader = ServiceLoader.load(ChunkedTransportProvider.class);
        
        for (ChunkedTransportProvider provider : loader) {
            TransportScheme annotation = provider.getClass().getAnnotation(TransportScheme.class);
            
            if (annotation != null) {
                String[] supportedSchemes = annotation.value();
                String urlScheme = urlObj.getProtocol();
                
                for (String supportedScheme : supportedSchemes) {
                    if (supportedScheme.equalsIgnoreCase(urlScheme)) {
                        // Found a matching provider for this scheme
                        try {
                            return provider.getClient(urlObj);
                        } catch (UnsupportedOperationException e) {
                            // This provider doesn't support the URL, try next
                            continue;
                        }
                    }
                }
            }
        }
        
        throw new IllegalArgumentException("No transport provider found for URL: " + url);
    }


    /// Creates a ChunkedTransportClient for the given URI.
    /// 
    /// This is a convenience method that converts the URI to a string and delegates
    /// to the main create method.
    /// 
    /// @param uri The URI to the data source
    /// @return A ChunkedTransportClient instance appropriate for the URI type
    /// @throws IllegalArgumentException if the URI is null or has an unsupported scheme
    /// @throws IOException if the URI cannot be accessed
    public static ChunkedTransportClient create(URI uri) throws IOException {
        if (uri == null) {
            throw new IllegalArgumentException("URI cannot be null");
        }
        return create(uri.toString());
    }

    /// Creates a ChunkedTransportClient for the given file path.
    /// 
    /// This is a convenience method specifically for local file paths.
    /// 
    /// @param path The path to the local file
    /// @return A ChunkedTransportClient instance for the specified path
    /// @throws IllegalArgumentException if the path is null
    /// @throws IOException if the file cannot be accessed
    public static ChunkedTransportClient create(Path path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        // Convert to file:// URL and use the main create method
        return create(path.toUri().toString());
    }
}