package io.nosqlbench.writers;

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

import io.nosqlbench.readers.DataType;
import io.nosqlbench.readers.Encoding;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load Writer implementations.
/// It allows finding writers based on their DataType and Encoding annotations.
public class WriterLookup {

    private static final ServiceLoader<Writer> serviceLoader = ServiceLoader.load(
        Writer.class);

    /// Finds a Writer implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the matching Writer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<Writer<T>> findWriter(Encoding.Type encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(writer -> (Writer<T>) writer);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<Writer> provider,
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

    /// Finds a Writer implementation that matches the specified encoding name and dataType.
    /// This method converts the encoding name to an Encoding.Type enum value.
    ///
    /// @param encodingName The encoding name to match with the Encoding annotation (will be converted to enum)
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the matching Writer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<Writer<T>> findWriter(String encodingName, Class<T> dataType) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findWriter(encoding, dataType);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Finds a Writer implementation that matches the specified encoding and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the writer with
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the instantiated Writer, or empty if none found or instantiation fails
    @SuppressWarnings("unchecked")
    public static <T> Optional<Writer<T>> findWriter(Encoding.Type encoding, Class<T> dataType, Path path) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .flatMap(provider -> instantiateWithPath(provider, path))
            .map(writer -> (Writer<T>) writer);
    }
    
    /// Finds a Writer implementation that matches the specified encoding name and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the writer with
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the instantiated Writer, or empty if none found or instantiation fails
    public static <T> Optional<Writer<T>> findWriter(String encodingName, Class<T> dataType, Path path) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findWriter(encoding, dataType, path);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Find a Writer implementation for float arrays with the specified encoding.
    /// This is a convenience method for the common case of writing float vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching Writer for float arrays, or empty if none found
    public static Optional<Writer<float[]>> findFloatWriter(Encoding.Type encoding) {
        return findWriter(encoding, float[].class);
    }
    
    /// Find a Writer implementation for float arrays with the specified encoding name.
    /// This is a convenience method for the common case of writing float vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching Writer for float arrays, or empty if none found
    public static Optional<Writer<float[]>> findFloatWriter(String encodingName) {
        return findWriter(encodingName, float[].class);
    }
    
    /// Find a Writer implementation for float arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the writer with
    /// @return An Optional containing the instantiated Writer for float arrays, or empty if none found
    public static Optional<Writer<float[]>> findFloatWriter(Encoding.Type encoding, Path path) {
        return findWriter(encoding, float[].class, path);
    }
    
    /// Find a Writer implementation for float arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the writer with
    /// @return An Optional containing the instantiated Writer for float arrays, or empty if none found
    public static Optional<Writer<float[]>> findFloatWriter(String encodingName, Path path) {
        return findWriter(encodingName, float[].class, path);
    }
    
    /// Find a Writer implementation for integer arrays with the specified encoding.
    /// This is a convenience method for the common case of writing integer vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching Writer for integer arrays, or empty if none found
    public static Optional<Writer<int[]>> findIntWriter(Encoding.Type encoding) {
        return findWriter(encoding, int[].class);
    }
    
    /// Find a Writer implementation for integer arrays with the specified encoding name.
    /// This is a convenience method for the common case of writing integer vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching Writer for integer arrays, or empty if none found
    public static Optional<Writer<int[]>> findIntWriter(String encodingName) {
        return findWriter(encodingName, int[].class);
    }
    
    /// Find a Writer implementation for integer arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the writer with
    /// @return An Optional containing the instantiated Writer for integer arrays, or empty if none found
    public static Optional<Writer<int[]>> findIntWriter(Encoding.Type encoding, Path path) {
        return findWriter(encoding, int[].class, path);
    }
    
    /// Find a Writer implementation for integer arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the writer with
    /// @return An Optional containing the instantiated Writer for integer arrays, or empty if none found
    public static Optional<Writer<int[]>> findIntWriter(String encodingName, Path path) {
        return findWriter(encodingName, int[].class, path);
    }
    
    /// Returns a stream of all available Writer providers.
    ///
    /// @return A stream of ServiceLoader.Provider<Writer>
    private static Stream<ServiceLoader.Provider<Writer>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }
    
    /// Checks if the provider has a matching DataType annotation.
    ///
    /// @param provider The Writer provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<Writer> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
    
    /// Attempts to instantiate a Writer using a constructor that takes a Path.
    ///
    /// @param provider The ServiceLoader.Provider for the Writer implementation
    /// @param path The path to pass to the constructor
    /// @return An Optional containing the instantiated Writer, or empty if instantiation fails
    private static Optional<Writer> instantiateWithPath(
        ServiceLoader.Provider<Writer> provider, Path path) {
        Class<?> writerClass = provider.type();
        try {
            // Look for a constructor that takes a Path
            Constructor<?> constructor = writerClass.getConstructor(Path.class);
            return Optional.of((Writer) constructor.newInstance(path));
        } catch (NoSuchMethodException e) {
            // Try to create an instance and then initialize it with the path
            try {
                Writer writer = provider.get();
                Method initMethod = writerClass.getMethod("init", Path.class);
                initMethod.invoke(writer, path);
                return Optional.of(writer);
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
