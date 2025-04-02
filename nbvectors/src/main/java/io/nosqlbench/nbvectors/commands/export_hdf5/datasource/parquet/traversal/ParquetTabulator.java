package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVisitor;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore;

// TODO: make this multithreaded, its tooooo slooooow
public class ParquetTabulator implements ParquetVisitor {

  private long totalRows =Long.MIN_VALUE;

  public long getRecordCount() {
    if (totalRows == Long.MIN_VALUE) {
      throw new RuntimeException("Unable to get count without traversal first.");
    }
    return this.totalRows;
  }

  @Override
  public void beforeAll() {
    this.totalRows =0;
  }

  @Override
  public Depth getTraversalDepth() {
    return Depth.PAGES;
  }

  @Override
  public void afterPage(BoundedPageStore pageStore) {
    long rowCount = pageStore.pageReadStore().getRowCount();
    this.totalRows +=rowCount;
  }

  @Override
  public void afterRoot(PathAggregator path) {
    System.out.println("row count up to " + this.totalRows + " for " + path.getRootPath().getFileName());
  }


}
