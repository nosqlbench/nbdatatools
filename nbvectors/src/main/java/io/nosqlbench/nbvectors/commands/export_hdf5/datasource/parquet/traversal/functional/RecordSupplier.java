package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal.functional;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;

import java.util.function.Supplier;

/// A supplier for [Group] from a [BoundedPageStore]
public class RecordSupplier implements Supplier<Group> {

  private final BoundedPageStore pageClosure;
  private final RecordReader<Group> reader;
  private final long count;
  private long remaining;

  /// create a supplier for [Group] from a [BoundedPageStore]
  /// @param pageClosure the [BoundedPageStore] to read from
  public RecordSupplier(BoundedPageStore pageClosure) {
    this.pageClosure = pageClosure;
    this.count = this.pageClosure.pageReadStore().getRowCount();
    this.remaining = count;

    pageClosure.pageReadStore().getRowCount();
    this.reader = this.pageClosure.messageColumnIO()
        .getRecordReader(pageClosure.pageReadStore(), pageClosure.recordMaterializer());
  }

  @Override
  public Group get() {
    remaining--;
    if (remaining < 0) {
      return null;
    }
    Group read = this.reader.read();
    return read;
  }
}
