package io.nosqlbench.nbvectors.common.parquet;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class PathSorterTest {

  @Test
  public void testParquetGrouping() {
    URL testdata = getClass().getClassLoader().getResource("testdata");
    assertThat(testdata).isNotNull();
    Path testDataDir = Path.of(testdata.getPath());
    PathSorter sorter = new PathSorter(testDataDir, "*");
    List<Path> actualOrderedPaths = sorter.getOrderedPaths();
    List<String> dirnames =
        actualOrderedPaths.stream().map(Path::getFileName).map(String::valueOf).toList();
    assertThat(dirnames).containsExactly("dir-123-bigger", "dir-987-smaller");
  }

}