package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetTraversal;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVectorsReaderTest;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.ParqueLoggingVisitor;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ParquetTabulatorTest {

  @Test
  public void testTabulation() {
    ParquetTabulator tabulator =
        new ParquetTabulator();
    ParqueLoggingVisitor parqueLoggingVisitor = new ParqueLoggingVisitor(tabulator, Level.INFO);
    ParquetTraversal traversal =
        new ParquetTraversal(List.of(ParquetVectorsReaderTest.SNAPSHOT_PATH));
    traversal.traverse(parqueLoggingVisitor);
    long recordCount = tabulator.getRecordCount();
    System.out.println("record count: " + recordCount);
  }

}