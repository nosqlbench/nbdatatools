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


import org.apache.parquet.io.RecordReader;

import java.util.Objects;

/// A record reader with a count
public class BoundedRecordReader<T> {
  private final RecordReader<T> reader;
  private final long count;

  public BoundedRecordReader(RecordReader<T> reader, long count) {
    this.reader = reader;
    this.count = count;
  }

  public RecordReader<T> reader() {
    return reader;
  }

  public long count() {
    return count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BoundedRecordReader<?> that = (BoundedRecordReader<?>) o;
    return count == that.count && Objects.equals(reader, that.reader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reader, count);
  }

  @Override
  public String toString() {
    return "BoundedRecordReader{" +
           "reader=" + reader +
           ", count=" + count +
           '}';
  }
}
