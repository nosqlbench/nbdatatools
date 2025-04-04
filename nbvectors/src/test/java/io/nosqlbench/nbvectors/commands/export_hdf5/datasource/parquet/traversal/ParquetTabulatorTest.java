package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

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


import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.ParquetTraversal;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVectorsReaderTest;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.ParqueLoggingVisitor;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ParquetTabulatorTest {

  @Disabled
  @Test
  public void testTabulation() {
    ParquetTabulator tabulator =
        new ParquetTabulator();
    ParqueLoggingVisitor parqueLoggingVisitor = new ParqueLoggingVisitor(tabulator, Level.INFO);
    ParquetTraversal traversal =
        new ParquetTraversal(List.of(ParquetVectorsReaderTest.SNAPSHOT_PATH), 10);
    traversal.traverse(parqueLoggingVisitor);
    long recordCount = tabulator.getRecordCount();
    System.out.println("record count: " + recordCount);
  }

}
