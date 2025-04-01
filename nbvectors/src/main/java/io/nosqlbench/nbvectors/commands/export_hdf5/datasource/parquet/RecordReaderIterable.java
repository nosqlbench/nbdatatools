package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class RecordReaderIterable implements Iterable<BoundedRecordReader<Group>> {
  private final Path path;

  public RecordReaderIterable(Path path) {
    this.path = path;
  }

  @Override
  public Iterator<BoundedRecordReader<Group>> iterator() {
    return new RecordReaderIterator(path);
  }

  private class RecordReaderIterator implements Iterator<BoundedRecordReader<Group>> {
    public final ParquetFileReader fileReader;
    public final MessageColumnIO columnIO;
    private final MessageType schema;
    private long remaining;
    private PageReadStore pageReadStore;

    public RecordReaderIterator(Path path) {
      InputFile inf = new LocalInputFile(path);
      try {
        this.fileReader = ParquetFileReader.open(inf);
        ParquetMetadata footer = fileReader.getFooter();
        this.schema = footer.getFileMetaData().getSchema();
        this.columnIO = new ColumnIOFactory().getColumnIO(schema);
//        this.recordCount = fileReader.getRecordCount();
//        this.remaining = recordCount;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }

    @Override
    public boolean hasNext() {
      readIfEmpty();
      return (this.pageReadStore != null);
    }

    private void readIfEmpty() {
      if (this.pageReadStore == null) {
        try {
          pageReadStore = fileReader.readNextRowGroup();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public BoundedRecordReader<Group> next() {
      readIfEmpty();
      long rowCount = pageReadStore.getRowCount();
      RecordReader<Group> recordReader =
          columnIO.getRecordReader(pageReadStore, new GroupRecordConverter(schema));
      this.pageReadStore=null;
      return new BoundedRecordReader<>(recordReader, rowCount);
    }

  }
}
