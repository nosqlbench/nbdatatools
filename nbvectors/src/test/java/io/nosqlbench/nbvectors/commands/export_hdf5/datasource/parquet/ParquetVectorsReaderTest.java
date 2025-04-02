package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

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


import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import org.junit.jupiter.api.Disabled;
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
      DS_PATH.resolve(Path.of("snapshots/37feace541fadccf70579e9f289c3cf8e8b186d7"));

  public static Path SECTION_PATH = SNAPSHOT_PATH.resolve(Path.of("en"));

  public static Path FILE = SECTION_PATH.resolve(Path.of("0000.parquet"));

  @Disabled
  @Test
  public void testParquetCompositeTraversal() {
    ParquetVectorsReader pvr =
        new ParquetVectorsReader(List.of(SECTION_PATH));
    for (float[] lifv : pvr) {
      System.out.println(Arrays.toString(lifv));
    }

  }

}
