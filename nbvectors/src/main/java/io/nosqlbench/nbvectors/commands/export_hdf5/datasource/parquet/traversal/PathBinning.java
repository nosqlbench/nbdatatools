package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// bin paths by a pattern
/// This is specialized around .parquet files for now, but this should be generalized.
///
/// By default, the binning pattern is [#DEFAULT_BINNING_PATTERN]. This means that the parent
/// directory of the file is used as the bin name.
public class PathBinning extends SimpleFileVisitor<Path> {
  private final Pattern regex;
  private int componentDepth = 0;

  /// get the binning result
  /// @return the binning result
  public BinningResult getBins() {
    return new BinningResult(bins);
  }

  private final Map<String, List<Path>> bins = new LinkedHashMap();

  /// create a new path binning instance with the default binning pattern
  /// @param paths
  ///     the paths to bin
  public PathBinning(List<Path> paths) {
    this(paths, DEFAULT_BINNING_PATTERN);
  }

  /// create a new path binning instance with a custom binning pattern
  /// @param paths
  ///     the paths to bin
  /// @param binningPattern
  ///     the binning pattern, which must include a named capture group named `
  ///     bin`, as in {@code ?<bin>}
  /// @throws RuntimeException if the binning pattern is invalid, or if the include file paths do
  ///  not have the same depth.
  public PathBinning(List<Path> paths, Pattern binningPattern) {
    this.regex = binningPattern;
    if (!regex.pattern().contains("?<bin>")) {
      throw new RuntimeException("Invalid binning pattern. There must be a named capture group "
                                 + "bin, as in ...?<bin>...");
    }
    for (Path path : paths) {
      try {
        Files.walkFileTree(path, Set.of(FileVisitOption.FOLLOW_LINKS), 100, this);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
    if (!path.toString().endsWith(".parquet")) {
      System.out.println("skipped:" + path);
      return super.visitFile(path, attrs);
    }

    int nameCount = path.getNameCount();
    if (componentDepth == 0) {
      componentDepth = nameCount;
    }
    if (componentDepth != nameCount) {
      throw new RuntimeException(
          "Binning files at different depths would produce undefined " + "results.");
    }

    String pathString = path.toString();
    Matcher matcher = regex.matcher(pathString);
    if (matcher.matches()) {
      String bin = matcher.group("bin");
      if (bin == null) {
        throw new RemoteException("bin pattern failed to match againt " + pathString);
      }
      this.bins.computeIfAbsent(bin, b -> new ArrayList<>()).add(path);
    }

    return super.visitFile(path, attrs);
  }

  private static Pattern DEFAULT_BINNING_PATTERN = Pattern.compile("""
      (?x)                      # Enable extended mode for comments and whitespace
      ^                         # Start of string
      (?:
        .*[\\\\/]               # Greedily match up to the last directory separator
        (?<bin>[^\\\\/]+)    # Capture only the last directory name (parent directory)
        [\\\\/]                 # Directory separator after the parent directory
      )?                        # End of optional parent directory group
      (?<file>[^\\\\/]+)        # Capture the file name (non-separator characters)
      $
      """);

  /// the result of binning paths
  /// @see PathBinning
  public static class BinningResult extends LinkedHashMap<String, List<Path>> {
    /// create a new binning result
    /// @param m the bins
    public BinningResult(Map<? extends String, ? extends List<Path>> m) {
      super(m);
    }

    /// get the bins as a map of parent directories to paths
    /// @return the bins as a map of parent directories to paths
    public Map<Path, List<Path>> toParentGroups() {
      Map<Path, List<Path>> results = new LinkedHashMap<>();
      forEach((k, v) -> {
        v.forEach(p -> {
          Path parent = p.getParent();
          if (parent == null) {
            parent = p.toAbsolutePath().getParent();
            if (parent == null) {
              throw new RuntimeException("unable to resolve parent of path " + p);
            }
          }
          results.computeIfAbsent(parent, n -> new ArrayList<>()).add(p);
        });
      });
      return results;
    }
  }
}
