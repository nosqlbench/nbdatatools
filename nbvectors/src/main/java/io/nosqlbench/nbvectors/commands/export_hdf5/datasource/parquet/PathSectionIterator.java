package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public class PathSectionIterator implements Iterator<Path> {
  private LinkedList<Iterator<Path>> traversal = new LinkedList<>();
  private LinkedList<Path> nodelist = new LinkedList<>();

  public PathSectionIterator(List<Path> paths) {
    Path path = paths.removeFirst();
    if (!Files.exists(path)) {
      System.err.println("non-existent path: '" + path + "'");
    }
    if (Files.isRegularFile(path)) {
      this.nodelist.add(path);
    }
    if (Files.isDirectory(path)) {
      try {
        DirectoryStream<Path> p = Files.newDirectoryStream(path);
        p.forEach(nodelist::add);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (nodelist.isEmpty()) {
      Iterator<Path> pi = traversal.peekLast();
      return pi!=null && pi.hasNext();
    }
    return true;
  }

  @Override
  public Path next() {
    Iterator<Path> pathIterator = traversal.peekLast();
    if (pathIterator!=null) {
      pathIterator.next();
    }
    Path path = nodelist.removeFirst();
    if (Files.isRegularFile(path)) {
      return path;
    }
    if (Files.isDirectory(path)) {
      traversal.add(new PathSectionIterator(List.of(path)));
      return next();
    }
    throw new RuntimeException("unrecognized path type:" + path);
  }
}
