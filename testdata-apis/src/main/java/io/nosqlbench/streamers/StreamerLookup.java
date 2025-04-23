package io.nosqlbench.streamers;

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

/// A utility class that uses ServiceLoader to discover and load Streamer implementations.
/// It allows finding streamers based on their DataType and Encoding annotations.
public class StreamerLookup {

    private static final ServiceLoader<Streamer> serviceLoader = ServiceLoader.load(
        Streamer.class);

    /// Finds a Streamer implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data streamed by the Streamer
    /// @return An Optional containing the matching Streamer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<Streamer<T>> findStreamer(Encoding.Type encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(streamer -> (Streamer<T>) streamer);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<Streamer> provider,
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

    /// Finds a Streamer implementation that matches the specified encoding name and dataType.
    /// This method converts the encoding name to an Encoding.Type enum value.
    ///
    /// @param encodingName The encoding name to match with the Encoding annotation (will be converted to enum)
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data streamed by the Streamer
    /// @return An Optional containing the matching Streamer, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<Streamer<T>> findStreamer(String encodingName, Class<T> dataType) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findStreamer(encoding, dataType);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Finds a Streamer implementation that matches the specified encoding and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the streamer with
    /// @param <T> The type of data streamed by the Streamer
    /// @return An Optional containing the instantiated Streamer, or empty if none found or instantiation fails
    @SuppressWarnings("unchecked")
    public static <T> Optional<Streamer<T>> findStreamer(Encoding.Type encoding, Class<T> dataType, Path path) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .flatMap(provider -> instantiateWithPath(provider, path))
            .map(streamer -> (Streamer<T>) streamer);
    }
    
    /// Finds a Streamer implementation that matches the specified encoding name and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the streamer with
    /// @param <T> The type of data streamed by the Streamer
    /// @return An Optional containing the instantiated Streamer, or empty if none found or instantiation fails
    public static <T> Optional<Streamer<T>> findStreamer(String encodingName, Class<T> dataType, Path path) {
        try {
            Encoding.Type encoding = Encoding.Type.valueOf(encodingName.toLowerCase());
            return findStreamer(encoding, dataType, path);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }
    
    /// Find a Streamer implementation for float arrays with the specified encoding.
    /// This is a convenience method for the common case of streaming float vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching Streamer for float arrays, or empty if none found
    public static Optional<Streamer<float[]>> findFloatStreamer(Encoding.Type encoding) {
        return findStreamer(encoding, float[].class);
    }
    
    /// Find a Streamer implementation for float arrays with the specified encoding name.
    /// This is a convenience method for the common case of streaming float vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching Streamer for float arrays, or empty if none found
    public static Optional<Streamer<float[]>> findFloatStreamer(String encodingName) {
        return findStreamer(encodingName, float[].class);
    }
    
    /// Find a Streamer implementation for float arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the streamer with
    /// @return An Optional containing the instantiated Streamer for float arrays, or empty if none found
    public static Optional<Streamer<float[]>> findFloatStreamer(Encoding.Type encoding, Path path) {
        return findStreamer(encoding, float[].class, path);
    }
    
    /// Find a Streamer implementation for float arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the streamer with
    /// @return An Optional containing the instantiated Streamer for float arrays, or empty if none found
    public static Optional<Streamer<float[]>> findFloatStreamer(String encodingName, Path path) {
        return findStreamer(encodingName, float[].class, path);
    }
    
    /// Find a Streamer implementation for integer arrays with the specified encoding.
    /// This is a convenience method for the common case of streaming integer vectors.
    ///
    /// @param encoding The encoding type to match
    /// @return An Optional containing the matching Streamer for integer arrays, or empty if none found
    public static Optional<Streamer<int[]>> findIntStreamer(Encoding.Type encoding) {
        return findStreamer(encoding, int[].class);
    }
    
    /// Find a Streamer implementation for integer arrays with the specified encoding name.
    /// This is a convenience method for the common case of streaming integer vectors.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @return An Optional containing the matching Streamer for integer arrays, or empty if none found
    public static Optional<Streamer<int[]>> findIntStreamer(String encodingName) {
        return findStreamer(encodingName, int[].class);
    }
    
    /// Find a Streamer implementation for integer arrays with the specified encoding,
    /// and instantiate it with the given path.
    ///
    /// @param encoding The encoding type to match
    /// @param path The path to initialize the streamer with
    /// @return An Optional containing the instantiated Streamer for integer arrays, or empty if none found
    public static Optional<Streamer<int[]>> findIntStreamer(Encoding.Type encoding, Path path) {
        return findStreamer(encoding, int[].class, path);
    }
    
    /// Find a Streamer implementation for integer arrays with the specified encoding name,
    /// and instantiate it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param path The path to initialize the streamer with
    /// @return An Optional containing the instantiated Streamer for integer arrays, or empty if none found
    public static Optional<Streamer<int[]>> findIntStreamer(String encodingName, Path path) {
        return findStreamer(encodingName, int[].class, path);
    }
    
    /// Returns a stream of all available Streamer providers.
    ///
    /// @return A stream of ServiceLoader.Provider<Streamer>
    private static Stream<ServiceLoader.Provider<Streamer>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }
    
    /// Checks if the provider has a matching DataType annotation.
    ///
    /// @param provider The Streamer provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<Streamer> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }
    
    /// Attempts to instantiate a Streamer using a constructor that takes a Path.
    ///
    /// @param provider The ServiceLoader.Provider for the Streamer implementation
    /// @param path The path to pass to the constructor
    /// @return An Optional containing the instantiated Streamer, or empty if instantiation fails
    private static Optional<Streamer> instantiateWithPath(
        ServiceLoader.Provider<Streamer> provider, Path path) {
        Class<?> streamerClass = provider.type();
        try {
            // Look for a constructor that takes a Path
            Constructor<?> constructor = streamerClass.getConstructor(Path.class);
            return Optional.of((Streamer) constructor.newInstance(path));
        } catch (NoSuchMethodException e) {
            // Try to create an instance and then initialize it with the path
            try {
                Streamer streamer = provider.get();
                Method initMethod = streamerClass.getMethod("init", Path.class);
                initMethod.invoke(streamer, path);
                return Optional.of(streamer);
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
