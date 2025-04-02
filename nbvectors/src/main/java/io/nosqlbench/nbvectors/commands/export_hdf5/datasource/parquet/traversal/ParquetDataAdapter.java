package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVectorsReader;
import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.FlatteningIterable;
import io.nosqlbench.nbvectors.common.adapters.Sized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParquetDataAdapter implements Iterable<float[]>, Sized {

  private final List<Path> paths;
  private final Iterable<ParquetVectorsReader> parquetVectorsReaders;
  private final Iterable<float[]> iterable;


  public ParquetDataAdapter(List<Path> paths) {
    this.paths = paths;
    this.parquetVectorsReaders = composeAggregatorsIterable();
    iterable = new FlatteningIterable<ParquetVectorsReader, float[]>(parquetVectorsReaders, f -> f);
  }

  public Iterable<ParquetVectorsReader> composeAggregatorsIterable() {
    int dirs = 0;
    int files = 0;
    for (Path path : paths) {
      if (Files.isDirectory(path)) {
        dirs++;
      } else if (Files.isRegularFile(path) || Files.isSymbolicLink(path)) {
        files++;
      } else {
        throw new RuntimeException("unhandled file type for '" + path + "'");
      }
    }
    if (dirs != 0 && files != 0) {
      throw new RuntimeException("unable to handle mixed dirs (" + dirs + ") and files (" + files
                                 + "). Either pass dirs which represent groupings, or files which represent the contents of a single grouping.");
    }
    if (files > 0) {
      return List.of(new ParquetVectorsReader(paths));
      //      aggregators.add(new ParquetVectorsReader(paths));
    } else {
      PathBinning binning = new PathBinning(paths);
      PathBinning.BinningResult bins = binning.getBins();
      Map<Path, List<Path>> parentGroups = bins.toParentGroups();

      PathsSorter sorter = new PathsSorter(parentGroups.keySet(), true);
      PathsSorter.SortedResults sorted =
          sorter.sorted(PathsSorter.BY_REVERSE_TOTAL_SIZE, PathsSorter.BY_NAME);
      System.out.println("sorted:\n");
      sorted.forEach(s -> System.out.println(s.getRootPath()));

      return sorted.stream()
          .map(a -> new ParquetVectorsReader(a.getFileList().stream().sorted().toList())).toList();
    }
  }

  @Override
  public Iterator<float[]> iterator() {
    return iterable.iterator();
  }

  @Override
  public int getSize() {
    long total = 0;

    for (ParquetVectorsReader parquetVectorsReader : this.parquetVectorsReaders) {
      if (parquetVectorsReader instanceof Sized sized) {
        total += sized.getSize();
      }
    }
    if (total > Integer.MAX_VALUE) {
      throw new RuntimeException("int overflow on long size: " + total);
    }
    return (int) total;
  }
}
