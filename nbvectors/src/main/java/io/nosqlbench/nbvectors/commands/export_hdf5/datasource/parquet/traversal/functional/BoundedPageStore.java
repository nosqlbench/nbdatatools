package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordMaterializer;

import java.util.function.Supplier;

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
