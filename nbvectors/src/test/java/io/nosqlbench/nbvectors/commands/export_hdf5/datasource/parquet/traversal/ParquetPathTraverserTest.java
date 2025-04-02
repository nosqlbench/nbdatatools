package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVectorsReaderTest;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetPathTraverserTest {

  @Test
  public void testParquetPathTraverserTest() {
    PathAggregator traverser =
        new PathAggregator(ParquetVectorsReaderTest.SNAPSHOT_PATH, true);
    long size = traverser.getTotalSizeInBytes();
    System.out.println("total size in bytes = " + size);
  }

  @Test
  public void testParquetGrouping() {
    URL testdata = getClass().getClassLoader().getResource("testdata");
    assertThat(testdata).isNotNull();
    Path testDataDir = Path.of(testdata.getPath());

    PathAggregator traverser =
        new PathAggregator(ParquetVectorsReaderTest.SNAPSHOT_PATH, true);

    traverser.getFileList();
    PathBinning binning = new PathBinning(traverser.getFileList());
    binning.getBins().forEach((k,v) -> {
      System.out.println("bin[" + k + "] paths=" + v.size());
    });

    Set<Path> paths = binning.getBins().toParentGroups().keySet();

    List<Path> sorted = new PathsSorter(paths).sorted().toPaths();
    System.out.println("sorted bin dirs:" + sorted);

  }
}