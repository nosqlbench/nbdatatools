package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

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
import java.util.function.Function;

public class PathToPagerClosure implements Function<InputFile, BoundedRecordReader> {

  @Override
  public BoundedRecordReader<Group> apply(InputFile inf) {
    try {
      ParquetFileReader parquetFileReader = ParquetFileReader.open(inf);
      ParquetMetadata parquetMetadata = parquetFileReader.getFooter();
      MessageType messageType = parquetMetadata.getFileMetaData().getSchema();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(messageType);
      PageReadStore pageReadStore = parquetFileReader.readNextRowGroup();
      GroupRecordConverter recordMaterializer = new GroupRecordConverter(messageType);
      RecordReader<Group> recordReader =
          columnIO.getRecordReader(pageReadStore, recordMaterializer);
      return new BoundedRecordReader<>(recordReader, pageReadStore.getRowCount());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
