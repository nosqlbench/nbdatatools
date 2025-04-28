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


import io.nosqlbench.nbvectors.datasource.parquet.traversal.functional.RecordSupplier;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.function.Supplier;

/// Capture the state of a page read store needed to read records from it
/// @param pageReadStore the page read store
/// @param messageColumnIO the column IO
/// @param recordMaterializer the record materializer
public record BoundedPageStore(
    PageReadStore pageReadStore,
    MessageColumnIO messageColumnIO,
    RecordMaterializer<Group> recordMaterializer
) implements Supplier<RecordSupplier>
{
  @Override
  public RecordSupplier get() {
    return new RecordSupplier(this);
  }
}
