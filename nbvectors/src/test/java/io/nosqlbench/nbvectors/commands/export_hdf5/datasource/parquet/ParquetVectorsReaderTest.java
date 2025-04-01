package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.build_hdf5.DataSourceAdapterTest;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetVectorsReaderTest {

  private static Path HF_CACHE = Path.of("/mnt/bulkstore/dswork_s3/huggingface/cache");

  public static Path DS_PATH =
      HF_CACHE.resolve(Path.of("datasets--Cohere--wikipedia-2023-11-embed-multilingual-v3"));

  public static Path SNAPSHOT_PATH =
      DS_PATH.resolve(Path.of("snapshots" + "/37feace541fadccf70579e9f289c3cf8e8b186d7"));

  public static Path SECTION_PATH = SNAPSHOT_PATH.resolve(Path.of("en"));

  public static Path FILE = SECTION_PATH.resolve(Path.of("0000.parquet"));



  @Test
  public void testParquetCompositeTraversal() {
    ParquetVectorsReader pvr =
        new ParquetVectorsReader(List.of(SECTION_PATH));
    for (float[] lifv : pvr) {
      System.out.println(Arrays.toString(lifv));
    }

  }

}