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

import java.io.IOException;
import java.net.URL;

/// Service Provider Interface for creating ChunkedTransportClient instances.
/// 
/// Implementations of this interface must have a no-args constructor to be
/// compatible with the Java ServiceLoader mechanism. The provider is responsible
/// for creating appropriate ChunkedTransportClient instances based on the given URL.
/// 
/// Implementations should be annotated with @TransportScheme to indicate which
/// URL schemes they support (e.g., "http", "https", "file").
public interface ChunkedTransportProvider {
    
    /// Creates a ChunkedTransportClient for the given URL.
    /// 
    /// @param url The URL to create a transport client for
    /// @return A ChunkedTransportClient instance configured for the given URL
    /// @throws IOException if the client cannot be created or the URL is invalid
    /// @throws UnsupportedOperationException if this provider doesn't support the URL's scheme
    ChunkedTransportClient getClient(URL url) throws IOException;
}