package io.nosqlbench.nbvectors.api.commands;

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
import io.nosqlbench.nbvectors.api.fileio.VectorFileArray;
import io.nosqlbench.nbvectors.api.fileio.VectorFileStore;
import io.nosqlbench.nbvectors.api.noncore.VectorRandomAccessReader;
import io.nosqlbench.nbvectors.api.services.DataType;
import io.nosqlbench.nbvectors.api.services.Encoding;
import io.nosqlbench.nbvectors.api.services.FileType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load SizedReader implementations.
/// It allows finding readers based on their DataType and Encoding annotations.
public class VectorFileIO {

    private static final ServiceLoader<VectorRandomAccessReader> serviceLoader = ServiceLoader.load(
        VectorRandomAccessReader.class);

    /// Finds a SizedReader implementation that matches the specified encoding and dataType.
    ///
    /// @param encoding The encoding type to match with the Encoding annotation
    /// @param dataType The class representing the data type to match with the DataType annotation
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the matching SizedReader, or empty if none found
    @SuppressWarnings("unchecked")
    public static <T> Optional<VectorRandomAccessReader<T>> findReader(FileType encoding, Class<T> dataType) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .map(ServiceLoader.Provider::get)
            .map(reader -> (VectorRandomAccessReader<T>) reader);
    }

    private static boolean matchesEncoding(
        ServiceLoader.Provider<VectorRandomAccessReader> provider,
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
    public static <T> Optional<VectorRandomAccessReader<T>> findReader(String encodingName, Class<T> dataType) {
        try {
            FileType encoding = FileType.valueOf(encodingName.toLowerCase());
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
    public static <T> Optional<VectorRandomAccessReader<T>> findReader(FileType encoding, Class<T> dataType, Path path) {
        return providers()
            .filter(provider -> matchesEncoding(provider, encoding) && matchesDataType(provider, dataType))
            .findFirst()
            .flatMap(provider -> instantiateWithPath(provider, path))
            .map(reader -> (VectorRandomAccessReader<T>) reader);
    }

    /// Finds a SizedReader implementation that matches the specified encoding name and dataType,
    /// and instantiates it with the given path.
    ///
    /// @param encodingName The encoding name to match (will be converted to enum)
    /// @param dataType The class representing the data type to match
    /// @param path The path to initialize the reader with
    /// @param <T> The type of data read by the SizedReader
    /// @return An Optional containing the instantiated SizedReader, or empty if none found or instantiation fails
    public static <T> Optional<VectorRandomAccessReader<T>> findReader(String encodingName, Class<T> dataType, Path path) {
        try {
            FileType encoding = FileType.valueOf(encodingName.toLowerCase());
            return findReader(encoding, dataType, path);
        } catch (IllegalArgumentException e) {
            // If the encoding name doesn't match any enum value, return empty
            return Optional.empty();
        }
    }

    // Convenience methods for specific types have been removed in favor of the generic parameterized methods

    /// Returns a stream of all available SizedReader providers.
    ///
    /// @return A stream of ServiceLoader.Provider<SizedReader>
    private static Stream<ServiceLoader.Provider<VectorRandomAccessReader>> providers() {
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    /// Checks if the provider has a matching DataType annotation.
    ///
    /// @param provider The SizedReader provider to check
    /// @param dataType The data type class to match
    /// @return true if the provider has a matching DataType annotation, false otherwise
    private static boolean matchesDataType(ServiceLoader.Provider<VectorRandomAccessReader> provider, Class<?> dataType) {
        Class<?> type = provider.type();
        DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
        return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
    }

    /// Attempts to instantiate a SizedReader using a constructor that takes a Path.
    ///
    /// @param provider The ServiceLoader.Provider for the SizedReader implementation
    /// @param path The path to pass to the constructor
    /// @return An Optional containing the instantiated SizedReader, or empty if instantiation fails
    private static Optional<VectorRandomAccessReader> instantiateWithPath(
        ServiceLoader.Provider<VectorRandomAccessReader> provider, Path path) {
        Class<?> readerClass = provider.type();
        try {
            // Look for a constructor that takes a Path
            Constructor<?> constructor = readerClass.getConstructor(Path.class);
            return Optional.of((VectorRandomAccessReader) constructor.newInstance(path));
        } catch (NoSuchMethodException e) {
            // Try to create an instance and then initialize it with the path
            try {
                VectorRandomAccessReader reader = provider.get();
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

    /// Returns a stream of all available BoundedVectorFileStream providers.
    ///
    /// @return A stream of ServiceLoader.Provider<BoundedVectorFileStream>
    private static Stream<ServiceLoader.Provider<BoundedVectorFileStream>> streamProviders() {
        ServiceLoader<BoundedVectorFileStream> serviceLoader = ServiceLoader.load(BoundedVectorFileStream.class);
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    /// Checks if the provider has a matching DataType and Encoding annotation.
    ///
    /// @param provider The provider to check
    /// @param type The encoding type to match
    /// @param dataType The data type class to match
    /// @return true if the provider has matching annotations, false otherwise
    private static boolean matchesTypeAndEncoding(
        ServiceLoader.Provider<?> provider, 
        FileType type, 
        Class<?> dataType
    ) {
        Class<?> providerClass = provider.type();
        DataType dataTypeAnnotation = providerClass.getAnnotation(DataType.class);
        Encoding encodingAnnotation = providerClass.getAnnotation(Encoding.class);

        return dataTypeAnnotation != null && encodingAnnotation != null &&
               dataTypeAnnotation.value().equals(dataType) &&
               encodingAnnotation.value() == type;
    }

    /// Creates a VectorFileArray for the specified file type and data class.
    /// This method finds an appropriate BoundedVectorFileStream implementation based on the provided type and class.
    ///
    /// @param type The file type/encoding to use
    /// @param aClass The class representing the data type
    /// @param outputFile The path to the file to open
    /// @param <T> The type of data in the array
    /// @return A VectorFileArray instance for the specified file
    /// @throws IllegalArgumentException if no suitable implementation is found
    /// @throws RuntimeException if initialization fails
    public static <T> VectorFileArray<T> vectorFileArray(
        FileType type,
        Class<T> aClass,
        Path outputFile
    )
    {
        return streamProviders()
            .filter(provider -> matchesTypeAndEncoding(provider, type, aClass))
            .findFirst()
            .map(provider -> {
                try {
                    // Get the instance only after we've confirmed it matches our criteria
                    BoundedVectorFileStream<?> stream = provider.get();

                    // Initialize the stream with the provided path
                    stream.open(outputFile);

                    // Create a wrapper that adapts BoundedVectorFileStream to VectorFileArray
                    return new io.nosqlbench.nbvectors.api.fileio.BoundedVectorFileStreamAdapter<>(stream, aClass);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize vector file array:" + e.getMessage(),
                        e);
                }
            })
            .orElseThrow(() -> new IllegalArgumentException(
                "No vector file array implementation found for type " + aClass.getSimpleName() +
                " and encoding " + type));
    }


    /// Returns a stream of all available VectorFileStore providers.
    ///
    /// @return A stream of ServiceLoader.Provider<VectorFileStore>
    private static Stream<ServiceLoader.Provider<VectorFileStore>> storeProviders() {
        ServiceLoader<VectorFileStore> serviceLoader = ServiceLoader.load(VectorFileStore.class);
        return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
    }

    /// Creates a VectorFileStore for the specified file type and data class.
    /// This method finds an appropriate VectorFileStore implementation based on the provided type and class.
    ///
    /// @param type The file type/encoding to use
    /// @param aClass The class representing the data type
    /// @param outputFile The path to the file to open
    /// @param <T> The type of data in the store
    /// @return An Optional containing the VectorFileStore instance, or empty if no suitable implementation is found
    /// @throws RuntimeException if initialization fails
    public static <T> Optional<VectorFileStore<T>> vectorFileStore(
        FileType type,
        Class<T> aClass,
        Path outputFile
    )
    {
        return storeProviders()
            .filter(provider -> matchesTypeAndEncoding(provider, type, aClass))
            .findFirst()
            .map(provider -> {
                try {
                    // Get the instance only after we've confirmed it matches our criteria
                    VectorFileStore<?> store = provider.get();

                    // Initialize the store with the provided path
                    store.open(outputFile);

                    // Cast to the correct type
                    @SuppressWarnings("unchecked")
                    VectorFileStore<T> typedStore = (VectorFileStore<T>) store;
                    return typedStore;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize vector file store", e);
                }
            });
    }


}
