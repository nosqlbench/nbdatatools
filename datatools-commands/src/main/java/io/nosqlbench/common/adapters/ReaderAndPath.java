package io.nosqlbench.common.adapters;

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


import io.nosqlbench.nbdatatools.api.services.Encoding;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.services.FileType;

import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.function.Predicate;

public class ReaderAndPath {
  
  private final String reader;
  private final Path source;
  
  public ReaderAndPath(String reader, Path source) {
    this.reader = reader;
    this.source = source;
  }
  
  /// @return the reader
  public String reader() {
    return reader;
  }
  
  /// @return the source path
  public Path source() {
    return source;
  }

  public ReaderAndPath(String spec) {
    this(getReader(spec),getPath(spec));
  }

  private static Path getPath(String spec) {
    String[] parts = spec.split(":", 2);
    if (parts.length == 2) {
      return Path.of(parts[1]);
    }
    return Path.of(spec);
  }

  private static String getReader(String spec) {
    String[] parts = spec.split(":", 2);
    if (parts.length == 2) {
      return parts[0];
    }
    String[] extension = spec.trim().split("\\.");
    return extension[extension.length - 1];
  }

  public <T> BoundedVectorFileStream<T> getSizedReader(Class<? extends T> type) {
    // Load all BoundedVectorFileStream implementations via SPI
    // Load all BoundedVectorFileStream implementations via SPI on the classpath
    ServiceLoader<BoundedVectorFileStream> readers = ServiceLoader.load(BoundedVectorFileStream.class);
    Predicate<Class<?>> filter = encodingFilter(getEncodingType(reader));
    // Find first matching implementation and cast to the generic type
    return readers.stream()
        .map(ServiceLoader.Provider::get)
        .filter(v -> filter.test(v.getClass()))
        .findFirst()
        .map(v -> (BoundedVectorFileStream<T>) v)
        .orElseThrow();
  }
  
  private static FileType getEncodingType(String encodingName) {
    try {
      // Convert string like "fvec" to enum Encoding.Type.fvec
      return FileType.valueOf(encodingName.toLowerCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported encoding type: " + encodingName, e);
    }
  }
      
      /**
       * Get the encoding type for this reader path
       * @return The encoding type enum value
       */
      public FileType getEncodingType() {
    return getEncodingType(reader);
      }

  private static Predicate<Class<?>> encodingFilter(FileType type) {
      return (Class<?> clazz) -> {
        // Get the Encoding annotation from the class
        Encoding encoding = clazz.getAnnotation(Encoding.class);
        if (encoding == null) {
          return false;
        }
        // Compare enum values using == for proper enum comparison
        return encoding.value() == type;
    };
  }
}
