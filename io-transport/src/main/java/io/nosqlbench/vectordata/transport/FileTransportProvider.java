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

import io.nosqlbench.nbdatatools.api.services.TransportScheme;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportProvider;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/// Provider for file-based ChunkedTransportClient instances.
/// 
/// This provider handles "file" scheme URLs and creates FileByteRangeFetcher
/// instances for local file access. The provider validates that files exist
/// and are readable before creating the client.
@TransportScheme("file")
public class FileTransportProvider implements ChunkedTransportProvider {
    
    /// No-args constructor required for ServiceLoader
    public FileTransportProvider() {
    }
    
    @Override
    public ChunkedTransportClient getClient(URL url) throws IOException {
        if (url == null) {
            throw new IllegalArgumentException("URL cannot be null");
        }
        
        String scheme = url.getProtocol();
        if (!"file".equalsIgnoreCase(scheme)) {
            throw new UnsupportedOperationException("This provider only supports file:// URLs, got: " + scheme);
        }
        
        // Convert URL to Path
        Path path;
        try {
            path = Paths.get(url.toURI());
        } catch (Exception e) {
            throw new IOException("Failed to convert URL to path: " + url, e);
        }
        
        // Validate the file
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + path);
        }
        if (!Files.isRegularFile(path)) {
            throw new IOException("Path is not a regular file: " + path);
        }
        if (!Files.isReadable(path)) {
            throw new IOException("File is not readable: " + path);
        }
        
        return new FileByteRangeFetcher(path);
    }
}