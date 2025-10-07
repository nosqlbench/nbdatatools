package io.nosqlbench.nbvectors.datasource.parquet.traversal.functional;

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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.Objects;
import java.util.function.Supplier;

/// Capture the state of a page read store needed to read records from it
public class BoundedPageStore implements Supplier<RecordSupplier> {
  private final PageReadStore pageReadStore;
  private final MessageColumnIO messageColumnIO;
  private final RecordMaterializer<Group> recordMaterializer;

  public BoundedPageStore(PageReadStore pageReadStore, MessageColumnIO messageColumnIO,
                         RecordMaterializer<Group> recordMaterializer) {
    this.pageReadStore = pageReadStore;
    this.messageColumnIO = messageColumnIO;
    this.recordMaterializer = recordMaterializer;
  }

  public PageReadStore pageReadStore() {
    return pageReadStore;
  }

  public MessageColumnIO messageColumnIO() {
    return messageColumnIO;
  }

  public RecordMaterializer<Group> recordMaterializer() {
    return recordMaterializer;
  }

  @Override
  public RecordSupplier get() {
    return new RecordSupplier(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BoundedPageStore that = (BoundedPageStore) o;
    return Objects.equals(pageReadStore, that.pageReadStore) &&
           Objects.equals(messageColumnIO, that.messageColumnIO) &&
           Objects.equals(recordMaterializer, that.recordMaterializer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageReadStore, messageColumnIO, recordMaterializer);
  }

  @Override
  public String toString() {
    return "BoundedPageStore{" +
           "pageReadStore=" + pageReadStore +
           ", messageColumnIO=" + messageColumnIO +
           ", recordMaterializer=" + recordMaterializer +
           '}';
  }
}
