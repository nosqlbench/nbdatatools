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


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load SizedReader implementations.
/// It allows finding readers based on their DataType and Encoding annotations.
public class SizedReaderLookup {

    private static final ServiceLoader<SizedReader> serviceLoader = ServiceLoader.load(
        SizedReader.class);

    /// Finds a SizedReader implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the matching SizedReader, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<SizedReader<T>> findReader(Encoding.Type encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(reader -> (SizedReader<T>) reader);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<SizedReader> provider,
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
    public static <T> Optional<SizedReader<T>> findReader(String encodingName, Class<T> dataType) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findReader(encoding, dataType);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Finds a SizedReader implementation that matches the specified encoding and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the reader with
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the instantiated SizedReader, or empty if none found or instantiation fails
    @SuppressWarnings("unchecked")
    public static <T> Optional<SizedReader<T>> findReader(Encoding.Type encoding, Class<T> dataType, Path path) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .flatMap(provider -> instantiateWithPath(provider, path))
            .map(reader -> (SizedReader<T>) reader);
    }
    
    /// Finds a SizedReader implementation that matches the specified encoding name and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the reader with
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the instantiated SizedReader, or empty if none found or instantiation fails
    public static <T> Optional<SizedReader<T>> findReader(String encodingName, Class<T> dataType, Path path) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findReader(encoding, dataType, path);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Find a SizedReader implementation for float arrays with the specified encoding.
    /// This is a convenience method for the common case of reading float vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching SizedReader for float arrays, or empty if none found
    public static Optional<SizedReader<float[]>> findFloatReader(Encoding.Type encoding) {
        return findReader(encoding, float[].class);
    }
    
    /// Find a SizedReader implementation for float arrays with the specified encoding name.
    /// This is a convenience method for the common case of reading float vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching SizedReader for float arrays, or empty if none found
    public static Optional<SizedReader<float[]>> findFloatReader(String encodingName) {
        return findReader(encodingName, float[].class);
    }
    
    /// Find a SizedReader implementation for float arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the reader with
    /// @return An Optional containing the instantiated SizedReader for float arrays, or empty if none found
    public static Optional<SizedReader<float[]>> findFloatReader(Encoding.Type encoding, Path path) {
        return findReader(encoding, float[].class, path);
    }
    
    /// Find a SizedReader implementation for float arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the reader with
    /// @return An Optional containing the instantiated SizedReader for float arrays, or empty if none found
    public static Optional<SizedReader<float[]>> findFloatReader(String encodingName, Path path) {
        return findReader(encodingName, float[].class, path);
    }
    
    /// Find a SizedReader implementation for integer arrays with the specified encoding.
    /// This is a convenience method for the common case of reading integer vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching SizedReader for integer arrays, or empty if none found
    public static Optional<SizedReader<int[]>> findIntReader(Encoding.Type encoding) {
        return findReader(encoding, int[].class);
    }
    
    /// Find a SizedReader implementation for integer arrays with the specified encoding name.
    /// This is a convenience method for the common case of reading integer vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching SizedReader for integer arrays, or empty if none found
    public static Optional<SizedReader<int[]>> findIntReader(String encodingName) {
        return findReader(encodingName, int[].class);
    }
    
    /// Find a SizedReader implementation for integer arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the reader with
    /// @return An Optional containing the instantiated SizedReader for integer arrays, or empty if none found
    public static Optional<SizedReader<int[]>> findIntReader(Encoding.Type encoding, Path path) {
        return findReader(encoding, int[].class, path);
    }
    
    /// Find a SizedReader implementation for integer arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the reader with
    /// @return An Optional containing the instantiated SizedReader for integer arrays, or empty if none found
    public static Optional<SizedReader<int[]>> findIntReader(String encodingName, Path path) {
        return findReader(encodingName, int[].class, path);
    }
    
    /// Returns a stream of all available SizedReader providers.
    ///
    /// @return A stream of ServiceLoader.Provider<SizedReader>
    private static Stream<ServiceLoader.Provider<SizedReader>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }
    
    /// Checks if the provider has a matching DataType annotation.
    ///
    /// @param provider The SizedReader provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<SizedReader> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
    
    /// Attempts to instantiate a SizedReader using a constructor that takes a Path.
    ///
    /// @param provider The ServiceLoader.Provider for the SizedReader implementation
    /// @param path The path to pass to the constructor
    /// @return An Optional containing the instantiated SizedReader, or empty if instantiation fails
    private static Optional<SizedReader> instantiateWithPath(
        ServiceLoader.Provider<SizedReader> provider, Path path) {
        Class<?> readerClass = provider.type();
        try {
            // Look for a constructor that takes a Path
            Constructor<?> constructor = readerClass.getConstructor(Path.class);
            return Optional.of((SizedReader) constructor.newInstance(path));
        } catch (NoSuchMethodException e) {
            // Try to create an instance and then initialize it with the path
            try {
                SizedReader reader = provider.get();
                Method initMethod = readerClass.getMethod("init", Path.class);
                initMethod.invoke(reader, path);
                return Optional.of(reader);
            } catch (Exception ex) {
                // Log or handle the error appropriately
                return Optional.empty();
            }
        } catch (Exception e) {
            // Log or handle the error appropriately
            return Optional.empty();
        }
    }
}
