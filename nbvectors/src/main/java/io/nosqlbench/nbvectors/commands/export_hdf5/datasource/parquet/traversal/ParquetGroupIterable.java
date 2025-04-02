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


import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional.BoundedRecordReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.RecordReader;

import java.util.Iterator;

/// Given a [PageReadStore], create iterators of [Group]
public class ParquetGroupIterable implements Iterable<Group> {

  private final BoundedRecordReader<Group> recordReader;

  /// create a new parquet group iterable
  /// @param recordReader the record reader to read from
  public ParquetGroupIterable(BoundedRecordReader<Group> recordReader) {
    this.recordReader = recordReader;
  }

  @Override
  public Iterator<Group> iterator() {
    return new ParquetGroupIterator(recordReader);
  }

  private class ParquetGroupIterator implements Iterator<Group> {
    private final RecordReader<Group> groupReader;
    private final long count;
    private long remaining;

    /// create a new parquet group iterator
    /// @param groupReader the record reader to read from
    public ParquetGroupIterator(BoundedRecordReader<Group> groupReader) {
      this.groupReader = groupReader.reader();
      this.count = groupReader.count();
      this.remaining = count;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0;
    }

    @Override
    public Group next() {
      remaining--;
      return groupReader.read();
    }
  }
}
