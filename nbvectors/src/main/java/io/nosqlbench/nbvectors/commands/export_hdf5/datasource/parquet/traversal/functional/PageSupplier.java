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


import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.function.Supplier;

/// A supplier for [BoundedPageStore] from a [InputFile]
public class PageSupplier implements Supplier<BoundedPageStore> {

  private final MessageColumnIO columnIO;
  private final ParquetFileReader parquetFileReader;
  private final GroupRecordConverter groupRecordConverter;

  /// create a supplier for [BoundedPageStore] from a [InputFile]
  /// @param inf the [InputFile] to read from
  public PageSupplier(InputFile inf) {
    try {
      this.parquetFileReader = ParquetFileReader.open(inf);
      ParquetMetadata parquetMetadata = parquetFileReader.getFooter();
      MessageType messageType = parquetMetadata.getFileMetaData().getSchema();
      columnIO = new ColumnIOFactory().getColumnIO(messageType);
      this.groupRecordConverter = new GroupRecordConverter(messageType);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public BoundedPageStore get() {
    PageReadStore pageReadStore = null;
    try {
      pageReadStore = this.parquetFileReader.readNextRowGroup();
      if (pageReadStore==null) {
        return null;
      }
      return new BoundedPageStore(pageReadStore,columnIO,groupRecordConverter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
