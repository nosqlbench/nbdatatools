package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.RecordReader;

import java.io.IOException;
import java.util.Iterator;

/// Given a [PageReadStore], create iterators of [Group]
public class ParquetGroupIterable implements Iterable<Group> {

  private final BoundedRecordReader<Group> recordReader;

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
