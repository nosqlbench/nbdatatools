package io.nosqlbench.nbvectors.jsonalyze;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.function.LongConsumer;
import java.util.regex.Pattern;

public class SimpleBoundaryFinder {
  public void findOffsets(Path path, LongConsumer offsetReader) {
    BufferedInputStream bis;
    try {
      bis = new BufferedInputStream(path.toUri().toURL().openStream());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Pattern boundary = Pattern.compile("\\}\s*(,)\s*\\{");

  }
}
