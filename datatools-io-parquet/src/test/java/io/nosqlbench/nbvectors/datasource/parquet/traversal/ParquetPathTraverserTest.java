package io.nosqlbench.nbvectors.datasource.parquet.traversal;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import io.nosqlbench.nbvectors.datasource.parquet.ParquetVectorsReaderTest;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathAggregator;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathBinning;
import io.nosqlbench.nbvectors.datasource.parquet.layout.PathsSorter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetPathTraverserTest {

  @Disabled
  @Test
  public void testParquetPathTraverserTest() {
    PathAggregator traverser =
        new PathAggregator(ParquetVectorsReaderTest.SNAPSHOT_PATH, true);
    long size = traverser.getTotalSizeInBytes();
    System.out.println("total size in bytes = " + size);
  }

  @Disabled
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
//    System.out.println("sorted bin dirs:" + sorted);

  }
}
