/// Service provider interfaces and annotations for extensible data tool functionality.
///
/// This package defines the core service contracts and extension points for the nbdatatools framework,
/// including file type identification, data type handling, encoding schemes, and transport mechanisms.
/// Implementations are typically discovered via Java's ServiceLoader mechanism.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.nbdatatools.api.services.BundledCommand}: Marker for discoverable CLI commands
/// - {@link io.nosqlbench.nbdatatools.api.services.FileType}: Enumeration of supported file formats
/// - {@link io.nosqlbench.nbdatatools.api.services.DataType}: Enumeration of data types
/// - {@link io.nosqlbench.nbdatatools.api.services.Encoding}: Enumeration of encoding schemes
/// - {@link io.nosqlbench.nbdatatools.api.services.FileExtension}: Annotation for extension-based discovery
/// - {@link io.nosqlbench.nbdatatools.api.services.TransportScheme}: Annotation for transport scheme binding
/// - {@link io.nosqlbench.nbdatatools.api.services.Selector}: Annotation for selector-based discovery
///
/// ## Usage Example
///
/// ```java
/// @FileExtension({".fvec", ".ivec"})
/// @TransportScheme({"file", "https"})
/// public class XvecReader implements VectorReader {
///     // implementation
/// }
/// ```
package io.nosqlbench.nbdatatools.api.services;

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
