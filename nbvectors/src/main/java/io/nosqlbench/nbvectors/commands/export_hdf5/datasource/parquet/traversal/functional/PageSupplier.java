package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

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

public class PageSupplier implements Supplier<BoundedPageStore> {

  private final MessageColumnIO columnIO;
  private final ParquetFileReader parquetFileReader;
  private final GroupRecordConverter groupRecordConverter;

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
//      System.out.println("page:" + pageReadStore);
      return new BoundedPageStore(pageReadStore,columnIO,groupRecordConverter);
//      RecordReader<Group> recordReader =
//          columnIO.getRecordReader(pageReadStore, recordMaterializer);
//      return new RecordSupplier(pageReadStore, recordReader, pageReadStore.getRowCount());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
