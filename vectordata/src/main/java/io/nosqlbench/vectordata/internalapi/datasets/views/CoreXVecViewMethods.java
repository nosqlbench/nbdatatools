package io.nosqlbench.vectordata.internalapi.datasets.views;

import io.nosqlbench.vectordata.api.Indexed;
import io.nosqlbench.vectordata.internalapi.datasets.DatasetView;
import io.nosqlbench.vectordata.layout.manifest.DSWindow;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class CoreXVecViewMethods<T> implements DatasetView<T> {

  private final Path path;
  private final SeekableByteChannel channel;
  private final DSWindow window;
  private Class<?> type;
  private long sourceSize;


  public CoreXVecViewMethods(Path path, long sourceSize, DSWindow window) {
    this.path = path;
    this.sourceSize = sourceSize;
    this.window = window;
    try {
      this.channel = Files.newByteChannel(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getCount() {
    return 0;
  }

  @Override
  public int getVectorDimensions() {
    return 0;
  }

  @Override
  public Class<?> getDataType() {
    return null;
  }

  @Override
  public T get(long index) {
    return null;
  }

  @Override
  public T[] getRange(long startInclusive, long endExclusive) {
    return null;
  }

  @Override
  public Indexed<T> getIndexed(long index) {
    return null;
  }

  @Override
  public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive) {
    return new Indexed[0];
  }

  @Override
  public List<T> toList() {
    return List.of();
  }

  @Override
  public <U> List<U> toList(Function<T, U> f) {
    return List.of();
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    return null;
  }
}
