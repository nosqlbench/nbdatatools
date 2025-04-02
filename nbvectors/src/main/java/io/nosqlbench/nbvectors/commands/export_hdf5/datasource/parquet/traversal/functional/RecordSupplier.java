package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

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


import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;

import java.util.function.Supplier;

/// A supplier for [Group] from a [BoundedPageStore]
public class RecordSupplier implements Supplier<Group> {

  private final BoundedPageStore pageClosure;
  private final RecordReader<Group> reader;
  private final long count;
  private long remaining;

  /// create a supplier for [Group] from a [BoundedPageStore]
  /// @param pageClosure the [BoundedPageStore] to read from
  public RecordSupplier(BoundedPageStore pageClosure) {
    this.pageClosure = pageClosure;
    this.count = this.pageClosure.pageReadStore().getRowCount();
    this.remaining = count;

    pageClosure.pageReadStore().getRowCount();
    this.reader = this.pageClosure.messageColumnIO()
        .getRecordReader(pageClosure.pageReadStore(), pageClosure.recordMaterializer());
  }

  @Override
  public Group get() {
    remaining--;
    if (remaining < 0) {
      return null;
    }
    Group read = this.reader.read();
    return read;
  }
}
