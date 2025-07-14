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


import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/// A utility class that uses ServiceLoader to discover and load SizedReader implementations.
/// It allows finding readers based on their DataType and Encoding annotations.
public class VectorFileIO {

  /// Construct a VectorFileIO instance.
  ///
  /// Private constructor to prevent instantiation of this utility class.
  private VectorFileIO() {
  }

  private static final ServiceLoader<VectorFileArray> serviceLoader =
      ServiceLoader.load(VectorFileArray.class);

  /// Finds a BoundedVectorFileStream implementation that matches the specified encoding and dataType,
  /// and instantiates it with the given path.
  /// @param encoding
  ///     The encoding type to match
  /// @param dataType
  ///     The class representing the data type to match
  /// @param path
  ///     The path to initialize the reader with
  /// @param <T>
  ///     The type of data read by the BoundedVectorFileStream
  /// @return An Optional containing the instantiated BoundedVectorFileStream, or empty if none found or
  ///  instantiation fails
  @SuppressWarnings("unchecked")
  public static <T> Optional<BoundedVectorFileStream<T>> streamIn(
      FileType encoding,
      Class<T> dataType,
      Path path
  )
  {
    String fileExtension = getFileExtension(path);

    // First, try to find a provider that matches both type/encoding and file extension
    List<ServiceLoader.Provider<BoundedVectorFileStream>> matchingProviders = streamProviders().stream()
        .filter(provider -> matchesTypeAndEncoding(provider, encoding, dataType))
        .toList();

    // Check for providers that match the extension
    List<ServiceLoader.Provider<BoundedVectorFileStream>> extensionMatches = matchingProviders.stream()
        .filter(provider -> matchesFileExtension(provider, fileExtension))
        .toList();

    // If we have providers that match the type/encoding but none match the extension, warn the user
    if (!extensionMatches.isEmpty()) {
      // We have providers that match both criteria
      return extensionMatches.stream().findFirst().map(p -> p.get()).map(reader -> {
        try {
          reader.open(path);
          return reader;
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize bounded vector file stream:" + e.getMessage(), e);
        }
      });
    } else if (!matchingProviders.isEmpty()) {
      // We have providers that match type/encoding but not the extension
      System.err.println("Warning: No readers found for extension '" + fileExtension + 
          "' that match the requested encoding and data type. Using best available match.");
      return matchingProviders.stream().findFirst().map(p -> p.get()).map(reader -> {
        try {
          reader.open(path);
          return reader;
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize bounded vector file stream:" + e.getMessage(), e);
        }
      });
    } else {
      // No matching providers at all
      return Optional.empty();
    }
  }

  /// Finds a BoundedVectorFileStream implementation that matches the specified encoding name and dataType,
  /// and instantiates it with the given path.
  /// @param encodingName
  ///     The encoding name to match (will be converted to enum)
  /// @param dataType
  ///     The class representing the data type to match
  /// @param path
  ///     The path to initialize the reader with
  /// @param <T>
  ///     The type of data read by the BoundedVectorFileStream
  /// @return An Optional containing the instantiated BoundedVectorFileStream, or empty if none found or
  ///  instantiation fails
  private static <T> Optional<BoundedVectorFileStream<T>> streamIn(
      String encodingName,
      Class<T> dataType,
      Path path
  )
  {
    try {
      FileType encoding = FileType.valueOf(encodingName.toLowerCase());
      return streamIn(encoding, dataType, path);
    } catch (IllegalArgumentException e) {
      // If the encoding name doesn't match any enum value, return empty
      return Optional.empty();
    }
  }

  private static boolean matchesEncoding(
      ServiceLoader.Provider<VectorFileArray> provider,
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

  // Convenience methods for specific types have been removed in favor of the generic parameterized methods

  /// Returns a stream of all available SizedReader providers.
  /// @return A stream of ServiceLoader.Provider<SizedReader>
  private static Stream<ServiceLoader.Provider<VectorFileArray>> providers() {
    return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
  }

  /// Checks if the provider has a matching DataType annotation.
  /// @param provider
  ///     The SizedReader provider to check
  /// @param dataType
  ///     The data type class to match
  /// @return true if the provider has a matching DataType annotation, false otherwise
  private static boolean matchesDataType(
      ServiceLoader.Provider<VectorFileArray> provider,
      Class<?> dataType
  )
  {
    Class<?> type = provider.type();
    DataType dataTypeAnnotation = type.getAnnotation(DataType.class);
    return dataTypeAnnotation != null && dataTypeAnnotation.value().equals(dataType);
  }

  /// Attempts to instantiate a SizedReader using a constructor that takes a Path.
  /// @param provider
  ///     The ServiceLoader.Provider for the SizedReader implementation
  /// @param path
  ///     The path to pass to the constructor
  /// @return An Optional containing the instantiated SizedReader, or empty if instantiation fails
  private static Optional<VectorFileArray> instantiateWithPath(
      ServiceLoader.Provider<VectorFileArray> provider,
      Path path
  )
  {
    Class<?> readerClass = provider.type();
    try {
      // Look for a constructor that takes a Path
      Constructor<?> constructor = readerClass.getConstructor(Path.class);
      return Optional.of((VectorFileArray) constructor.newInstance(path));
    } catch (NoSuchMethodException e) {
      // Try to create an instance and then initialize it with the path
      try {
        VectorFileArray reader = provider.get();
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
  /// @return A stream of ServiceLoader.Provider<BoundedVectorFileStream>
  private static List<ServiceLoader.Provider<BoundedVectorFileStream>> streamProviders() {
    ServiceLoader<BoundedVectorFileStream> serviceLoader =
        ServiceLoader.load(BoundedVectorFileStream.class);
    List<ServiceLoader.Provider<BoundedVectorFileStream>> providers =
        StreamSupport.stream(serviceLoader.stream().spliterator(), false).toList();
    return providers;
  }

  /// Checks if the provider has a matching DataType and Encoding annotation.
  /// @param provider
  ///     The provider to check
  /// @param type
  ///     The encoding type to match
  /// @param dataType
  ///     The data type class to match
  /// @return true if the provider has matching annotations, false otherwise
  private static boolean matchesTypeAndEncoding(
      ServiceLoader.Provider<?> provider,
      FileType type,
      Class<?> dataType
  )
  {
    Class<?> providerClass = provider.type();
    DataType dataTypeAnnotation = providerClass.getAnnotation(DataType.class);
    Encoding encodingAnnotation = providerClass.getAnnotation(Encoding.class);

    return dataTypeAnnotation != null && encodingAnnotation != null && dataTypeAnnotation.value()
        .equals(dataType) && encodingAnnotation.value() == type;
  }

  /// Extract the file extension from a path
  /// @param path The path to extract the extension from
  /// @return The file extension (including the dot)
  private static String getFileExtension(Path path) {
    String fileName = path.getFileName().toString();
    int lastDotIndex = fileName.lastIndexOf('.');
    return lastDotIndex > 0 ? fileName.substring(lastDotIndex) : "";
  }

  /// Check if a provider matches a file extension
  /// @param provider The provider to check
  /// @param extension The file extension to match (including the dot)
  /// @return true if the provider has a matching FileExtension annotation, false otherwise
  private static boolean matchesFileExtension(
      ServiceLoader.Provider<?> provider,
      String extension
  ) {
    if (extension.isEmpty()) {
      return true; // If no extension is provided, consider it a match
    }

    Class<?> providerClass = provider.type();
    FileExtension fileExtensionAnnotation = providerClass.getAnnotation(FileExtension.class);

    if (fileExtensionAnnotation != null) {
      return Arrays.asList(fileExtensionAnnotation.value()).contains(extension.toLowerCase());
    }

    return false;
  }

  /// Creates a VectorFileArray for the specified file type and data class.
  /// This method finds an appropriate BoundedVectorFileStream implementation based on the provided
  /// type and class.
  /// @param type
  ///     The file type/encoding to use
  /// @param aClass
  ///     The class representing the data type
  /// @param outputFile
  ///     The path to the file to open
  /// @param <T>
  ///     The type of data in the array
  /// @return A VectorFileArray instance for the specified file
  /// @throws IllegalArgumentException
  ///     if no suitable implementation is found
  /// @throws RuntimeException
  ///     if initialization fails
  public static <T> VectorFileArray<T> randomAccess(
      FileType type,
      Class<T> aClass,
      Path outputFile
  )
  {
    String fileExtension = getFileExtension(outputFile);

    // Find providers that match the type/encoding
    List<ServiceLoader.Provider<VectorFileArray>> matchingProviders = providers()
        .filter(provider -> matchesTypeAndEncoding(provider, type, aClass))
        .toList();

    // Filter to find providers that also match the file extension
    List<ServiceLoader.Provider<VectorFileArray>> extensionMatches = matchingProviders.stream()
        .filter(provider -> matchesFileExtension(provider, fileExtension))
        .toList();

    if (!extensionMatches.isEmpty()) {
      // We have providers that match both criteria
      return extensionMatches.stream().findFirst().map(provider -> {
        try {
          // Get the instance only after we've confirmed it matches our criteria
          VectorFileArray<T> reader = provider.get();

          // Initialize the stream with the provided path
          reader.open(outputFile);
          return reader;
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to initialize vector file array:" + e.getMessage(),
              e
          );
        }
      }).orElseThrow(() -> new IllegalArgumentException(
          "Failed to initialize vector file array for type " + aClass.getSimpleName()
          + " and encoding " + type));
    } else if (!matchingProviders.isEmpty()) {
      // We have providers that match type/encoding but not the extension
      System.err.println("Warning: No readers found for extension '" + fileExtension + 
          "' that match the requested encoding and data type. Using best available match.");
      return matchingProviders.stream().findFirst().map(provider -> {
        try {
          // Get the instance only after we've confirmed it matches our criteria
          VectorFileArray<T> reader = provider.get();

          // Initialize the stream with the provided path
          reader.open(outputFile);
          return reader;
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to initialize vector file array:" + e.getMessage(),
              e
          );
        }
      }).orElseThrow(() -> new IllegalArgumentException(
          "Failed to initialize vector file array for type " + aClass.getSimpleName()
          + " and encoding " + type));
    } else {
      // No matching providers at all
      throw new IllegalArgumentException(
          "No vector file array implementation found for type " + aClass.getSimpleName()
          + " and encoding " + type);
    }
  }


  /// Returns a stream of all available VectorFileStore providers.
  /// @return A stream of ServiceLoader.Provider<VectorFileStore>
  private static Stream<ServiceLoader.Provider<VectorFileStreamStore>> storeProviders() {
    ServiceLoader<VectorFileStreamStore> serviceLoader = ServiceLoader.load(VectorFileStreamStore.class);
    return StreamSupport.stream(serviceLoader.stream().spliterator(), false);
  }


  /// Creates a VectorFileStore for the specified file type and data class.
  /// This method finds an appropriate VectorFileStore implementation based on the provided type and
  /// class.
  /// @param type
  ///     The file type/encoding to use
  /// @param aClass
  ///     The class representing the data type
  /// @param outputFile
  ///     The path to the file to open
  /// @param <T>
  ///     The type of data in the store
  /// @return An Optional containing the VectorFileStore instance, or empty if no suitable
  ///  implementation is found
  /// @throws RuntimeException
  ///     if initialization fails
  public static <T> Optional<VectorFileStreamStore<T>> streamOut(
      FileType type,
      Class<T> aClass,
      Path outputFile
  )
  {
    String fileExtension = getFileExtension(outputFile);

    // Find providers that match the type/encoding
    List<ServiceLoader.Provider<VectorFileStreamStore>> matchingProviders = storeProviders()
        .filter(provider -> matchesTypeAndEncoding(provider, type, aClass))
        .toList();

    // Filter to find providers that also match the file extension
    List<ServiceLoader.Provider<VectorFileStreamStore>> extensionMatches = matchingProviders.stream()
        .filter(provider -> matchesFileExtension(provider, fileExtension))
        .toList();

    if (!extensionMatches.isEmpty()) {
      // We have providers that match both criteria
      return extensionMatches.stream().findFirst().map(provider -> {
        try {
          // Get the instance only after we've confirmed it matches our criteria
          VectorFileStreamStore<?> store = provider.get();

          // Initialize the store with the provided path
          store.open(outputFile);

          // Cast to the correct type
          @SuppressWarnings("unchecked") VectorFileStreamStore<T> typedStore =
              (VectorFileStreamStore<T>) store;
          return typedStore;
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize vector file store", e);
        }
      });
    } else if (!matchingProviders.isEmpty()) {
      // We have providers that match type/encoding but not the extension
      System.err.println("Warning: No writers found for extension '" + fileExtension + 
          "' that match the requested encoding and data type. Using best available match, but the file extension may be incorrect.");
      return matchingProviders.stream().findFirst().map(provider -> {
        try {
          // Get the instance only after we've confirmed it matches our criteria
          VectorFileStreamStore<?> store = provider.get();

          // Initialize the store with the provided path
          store.open(outputFile);

          // Cast to the correct type
          @SuppressWarnings("unchecked") VectorFileStreamStore<T> typedStore =
              (VectorFileStreamStore<T>) store;
          return typedStore;
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize vector file store", e);
        }
      });
    } else {
      // No matching providers at all
      return Optional.empty();
    }
  }


}
