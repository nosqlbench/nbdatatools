package io.nosqlbench.readers;

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


import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load SizedReader implementations.
/// It allows finding readers based on their DataType and Selector annotations.
public class SizedStreamerLookup {

    private static final ServiceLoader<SizedStreamer> serviceLoader = ServiceLoader.load(
        SizedStreamer.class);

    /// Finds a SizedReader implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the matching SizedReader, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<SizedStreamer<T>> findReader(Encoding.Type encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(reader -> (SizedStreamer<T>) reader);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<SizedStreamer> provider,
        Encoding.Type encoding
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
    public static <T> Optional<SizedStreamer<T>> findReader(String encodingName, Class<T> dataType) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findReader(encoding, dataType);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Find a SizedReader implementation for float arrays with the specified encoding.
    /// This is a convenience method for the common case of reading float vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching SizedReader for float arrays, or empty if none found
    public static Optional<SizedStreamer<float[]>> findFloatVectorReader(String encodingName) {
        return findReader(encodingName, float[].class);
    }
    
    /// Find a SizedReader implementation for integer arrays with the specified encoding.
    /// This is a convenience method for the common case of reading integer vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching SizedReader for integer arrays, or empty if none found
    public static Optional<SizedStreamer<int[]>> findIntVectorReader(String encodingName) {
        return findReader(encodingName, int[].class);
    }
    
    /// Returns a stream of all available SizedReader providers.
    ///
    /// @return A stream of ServiceLoader.Provider<SizedReader>
    private static Stream<ServiceLoader.Provider<SizedStreamer>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }
    
    ///
    /// @param provider The SizedReader provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<SizedStreamer> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
}
