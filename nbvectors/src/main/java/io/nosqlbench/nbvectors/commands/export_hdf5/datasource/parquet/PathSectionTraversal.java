package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import com.google.common.math.StatsAccumulator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PathSectionTraversal implements Iterable<Path>{

  private final List<Path> paths = new LinkedList<>();

  public PathSectionTraversal(List<Path> paths) {
    this.paths.addAll(paths);

  }

  @Override
  public Iterator<Path> iterator() {
    return new PathSectionIterator(paths);
  }
}
