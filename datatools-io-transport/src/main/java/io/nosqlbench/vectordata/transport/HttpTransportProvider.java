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

/// Provider for HTTP/HTTPS-based ChunkedTransportClient instances.
/// 
/// This provider handles "http" and "https" scheme URLs and creates
/// HttpByteRangeFetcher instances for remote resource access via HTTP.
@TransportScheme({"http", "https"})
public class HttpTransportProvider implements ChunkedTransportProvider {
    
    /// No-args constructor required for ServiceLoader
    public HttpTransportProvider() {
    }
    
    @Override
    public ChunkedTransportClient getClient(URL url) throws IOException {
        if (url == null) {
            throw new IllegalArgumentException("URL cannot be null");
        }
        
        String scheme = url.getProtocol();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new UnsupportedOperationException("This provider only supports http/https URLs, got: " + scheme);
        }
        
        return new HttpByteRangeFetcher(url.toString());
    }
}