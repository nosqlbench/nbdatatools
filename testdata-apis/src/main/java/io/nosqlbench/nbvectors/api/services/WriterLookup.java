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

import io.nosqlbench.nbvectors.api.fileio.VectorWriter;

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

    private static final ServiceLoader<VectorWriter> serviceLoader = ServiceLoader.load(
        VectorWriter.class);

    /// Finds a Writer implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the matching Writer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<VectorWriter<T>> findWriter(FileType encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(writer -> (VectorWriter<T>) writer);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<VectorWriter> provider,
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

    /// Finds a Writer implementation that matches the specified encoding name and dataType.
    /// This method converts the encoding name to an Encoding.Type enum value.
    ///
    /// @param encodingName The encoding name to match with the Encoding annotation (will be converted to enum)
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the matching Writer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<VectorWriter<T>> findWriter(String encodingName, Class<T> dataType) {
        try {
            FileType encoding = FileType.valueOf(encodingName.toLowerCase());
            Optional<VectorWriter<T>> writer = findWriter(encoding, dataType);
            if (writer.isPresent()) {
                return writer;
            }
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, continue to direct lookup
        }
        
        // Try direct class loading as fallback
        return findWriterByClassName(encodingName, dataType);
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
    public static <T> Optional<VectorWriter<T>> findWriter(FileType encoding, Class<T> dataType, Path path) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .flatMap(provider -> instantiateWithPath(provider, path))
            .map(writer -> (VectorWriter<T>) writer);
    }
    
    /// Finds a Writer implementation that matches the specified encoding name and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the writer with
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the instantiated Writer, or empty if none found or instantiation fails
    public static <T> Optional<VectorWriter<T>> findWriter(String encodingName, Class<T> dataType, Path path) {
        try {
            FileType encoding = FileType.valueOf(encodingName.toLowerCase());
            Optional<VectorWriter<T>> writer = findWriter(encoding, dataType, path);
            if (writer.isPresent()) {
                return writer;
            }
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, continue to direct lookup
        }
        
        // Try direct class loading as fallback
        return findWriterByClassName(encodingName, dataType, path);
    }

    /// Creates a Writer instance directly by class name, bypassing SPI.
    ///
    /// @param className The fully qualified class name of the Writer implementation
    /// @param dataType The data type class expected
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the instantiated Writer, or empty if instantiation fails
    @SuppressWarnings("unchecked")
    public static <T> Optional<VectorWriter<T>> findWriterByClassName(String className, Class<T> dataType) {
        try {
            Class<?> writerClass = Class.forName(className);
            if (!VectorWriter.class.isAssignableFrom(writerClass)) {
                return Optional.empty(); // Not a Writer implementation
            }
            
            // Verify data type compatibility if annotated
            DataType dataTypeAnnotation = writerClass.getAnnotation(DataType.class);
            if (dataTypeAnnotation != null && !dataTypeAnnotation.value().equals(dataType)) {
                return Optional.empty(); // Data type mismatch
            }
            
            // Create instance using default constructor
            Constructor<?> constructor = writerClass.getConstructor();
            VectorWriter<T> writer = (VectorWriter<T>) constructor.newInstance();
            return Optional.of(writer);
        } catch (Exception e) {
            // Log error or handle appropriately
            return Optional.empty();
        }
    }
    
    /// Creates a Writer instance directly by class name with initialization path, bypassing SPI.
    ///
    /// @param className The fully qualified class name of the Writer implementation
    /// @param dataType The data type class expected
    /// @param path The path to initialize the writer with
    /// @param <T> The type of data written by the Writer
    /// @return An Optional containing the instantiated Writer, or empty if instantiation fails
    @SuppressWarnings("unchecked")
    public static <T> Optional<VectorWriter<T>> findWriterByClassName(String className, Class<T> dataType, Path path) {
        try {
            Class<?> writerClass = Class.forName(className);
            if (!VectorWriter.class.isAssignableFrom(writerClass)) {
                return Optional.empty(); // Not a Writer implementation
            }
            
            // Verify data type compatibility if annotated
            DataType dataTypeAnnotation = writerClass.getAnnotation(DataType.class);
            if (dataTypeAnnotation != null && !dataTypeAnnotation.value().equals(dataType)) {
                return Optional.empty(); // Data type mismatch
            }
            
            VectorWriter<T> writer = null;
            
            // Try path constructor first
            try {
                Constructor<?> constructor = writerClass.getConstructor(Path.class);
                writer = (VectorWriter<T>) constructor.newInstance(path);
                return Optional.of(writer);
            } catch (NoSuchMethodException e) {
                // Fall back to default constructor with initialize
                writer = (VectorWriter<T>) writerClass.getConstructor().newInstance();
            }
            
            // Try initialize method
            try {
                writer.open(path);
                return Optional.of(writer);
            } catch (Exception e) {
                // Try legacy init method
                try {
                    Method initMethod = writerClass.getMethod("init", Path.class);
                    initMethod.invoke(writer, path);
                    return Optional.of(writer);
                } catch (Exception ex) {
                    return Optional.empty();
                }
            }
        } catch (Exception e) {
            // Log error or handle appropriately
            return Optional.empty();
        }
    }
    
    // Convenience methods for specific types have been removed in favor of the generic parameterized methods
    
    /// Returns a stream of all available Writer providers.
    ///
    /// @return A stream of ServiceLoader.Provider<Writer>
    private static Stream<ServiceLoader.Provider<VectorWriter>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }
    
    /// Checks if the provider has a matching DataType annotation.
    ///
    /// @param provider The Writer provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<VectorWriter> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
    
    /// Attempts to instantiate a Writer using a constructor that takes a Path.
    ///
    /// @param provider The ServiceLoader.Provider for the Writer implementation
    /// @param path The path to pass to the constructor
    /// @return An Optional containing the instantiated Writer, or empty if instantiation fails
    private static Optional<VectorWriter> instantiateWithPath(
        ServiceLoader.Provider<VectorWriter> provider, Path path) {
        Class<?> writerClass = provider.type();
        try {
            // First try to use a constructor that takes a Path
            try {
                Constructor<?> constructor = writerClass.getConstructor(Path.class);
                return Optional.of((VectorWriter) constructor.newInstance(path));
            } catch (NoSuchMethodException e) {
                // If no such constructor exists, create an instance and call initialize
                VectorWriter writer = provider.get();
                writer.open(path);
                return Optional.of(writer);
            }
        } catch (Exception e) {
            // Try fallback to legacy 'init' method for backward compatibility
            try {
                VectorWriter writer = provider.get();
                Method initMethod = writerClass.getMethod("init", Path.class);
                initMethod.invoke(writer, path);
                return Optional.of(writer);
            } catch (Exception ex) {
                // Log or handle the error appropriately
                return Optional.empty();
            }
        }
    }
}
