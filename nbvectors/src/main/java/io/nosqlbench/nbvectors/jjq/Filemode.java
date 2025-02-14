package io.nosqlbench.nbvectors.jjq;

import java.nio.file.Files;
import java.nio.file.Path;

public enum Filemode {
  /// If a function has an output file option, and the file exists already, then presume that the
  /// function ran successfully before
  checkpoint,
  /// Overwrite any output files, truncating them first if they exist
  overwrite,
  /// If an output file already exists, cancel the operation and throw an error
  preserve;

  public boolean isSkip(Path path) {
    if (Files.exists(path)) {
      return switch (this) {
        case preserve -> throw new RuntimeException(
            "file 'path' exists, and " + this.getClass().getSimpleName() + "=" + this);
        case overwrite -> false;
        case checkpoint -> true;
      };
    } else {
      return false;
    }
  }
}
