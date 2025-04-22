package io.nosqlbench.nbvectors.common.adapters;

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


import io.nosqlbench.nbvectors.services.Selector;
import io.nosqlbench.readers.SizedReader;

import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.function.Predicate;

public record ReaderAndPath(String reader, Path source) {

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

  public <T> SizedReader<T> getSizedReader(Class<? extends T> type) {
    ServiceLoader<SizedReader<T>> readers =
        (ServiceLoader<SizedReader<T>>) ServiceLoader.load(type);
    Predicate<Class<?>> filter = selectorFilter(reader);
    ServiceLoader.Provider<SizedReader<T>> provider =
        readers.stream().filter(s -> filter.test(s.type())).findFirst().orElseThrow();
    return provider.get();
  }

  private static Predicate<Class<?>> selectorFilter(String string) {
    return (Class<?> clazz) -> {
      Selector selector = clazz.getAnnotation(Selector.class);
      if (selector == null) {
        return false;
      }
      return selector.value().equals(string);
    };
  }
}
