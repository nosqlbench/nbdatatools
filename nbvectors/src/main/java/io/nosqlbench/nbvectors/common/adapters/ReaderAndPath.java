package io.nosqlbench.nbvectors.common.adapters;

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
