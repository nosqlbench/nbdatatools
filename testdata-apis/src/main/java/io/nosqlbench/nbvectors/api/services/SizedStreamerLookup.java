package io.nosqlbench.nbvectors.api.services;

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


import io.nosqlbench.nbvectors.api.fileio.BoundedVectorFileStream;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load SizedReader implementations.
/// It allows finding readers based on their DataType and Selector annotations.
public class SizedStreamerLookup {

    /// Construct a SizedStreamerLookup instance.
    ///
    /// Private constructor to prevent instantiation of this utility class.
    private SizedStreamerLookup() {}

    private static final ServiceLoader<BoundedVectorFileStream> serviceLoader = ServiceLoader.load(
        BoundedVectorFileStream.class);

    /// Finds a SizedReader implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the matching SizedReader, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<BoundedVectorFileStream<T>> findReader(FileType encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(reader -> (BoundedVectorFileStream<T>) reader);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<BoundedVectorFileStream> provider,
        FileType encoding
    )
    {
        Class<?> type = provider.type();
        Encoding encodingAnnotation = type.getAnnotation(Encoding.class);
        if (encodingAnnotation != null) {
            return (encodingAnnotation.value() == encoding);
        }
        return false;
    }

    /// Finds a SizedReader implementation that matches the specified encoding name and dataType.
    /// This method converts the encoding name to an Encoding.Type enum value.
    ///
    /// @param encodingName The encoding name to match with the Encoding annotation (will be converted to enum)
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the matching SizedReader, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<BoundedVectorFileStream<T>> findReader(String encodingName, Class<T> dataType) {
        try {
            FileType encoding = FileType.valueOf(encodingName.toLowerCase());
            return findReader(encoding, dataType);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }

    // Convenience methods for specific types have been removed in favor of the generic parameterized methods

    /// Returns a stream of all available SizedReader providers.
    ///
    /// @return A stream of ServiceLoader.Provider<SizedReader>
    private static Stream<ServiceLoader.Provider<BoundedVectorFileStream>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    ///
    /// @param provider The SizedReader provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<BoundedVectorFileStream> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
}
