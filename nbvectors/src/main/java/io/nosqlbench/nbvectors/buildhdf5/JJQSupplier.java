package io.nosqlbench.nbvectors.buildhdf5;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class JJQSupplier {
  public static Supplier<String> path(Path trainingJsonFile) {
    try {
      List<String> lines = Files.readAllLines(trainingJsonFile);
      return new LinesSupplier(lines);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class LinesSupplier implements Supplier<String> {

    private final Iterable<String> source;
    private final Iterator<String> iterator;

    public LinesSupplier(Iterable<String> source) {
      this.source = source;
      this.iterator = source.iterator();
    }

    @Override
    public String get() {
      if (iterator.hasNext()) {
        return iterator.next();
      }
      return null;
    }
  }
}
