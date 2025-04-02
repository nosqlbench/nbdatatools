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


import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.ParquetVisitor;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedPageStore;

// TODO: make this multithreaded, its tooooo slooooow

/// Tabulate parquet data file statistics via traversal
public class ParquetTabulator implements ParquetVisitor {

  private long totalRows =Long.MIN_VALUE;

  /// get the total number of records seen during traversal
  /// @return the total number of records seen during traversal
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
