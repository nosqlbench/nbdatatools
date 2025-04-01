package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.jjq.bulkio.ConvertingIterable;
import io.nosqlbench.nbvectors.commands.jjq.bulkio.FlatteningIterable;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.common.parquet.PathSorter;
import org.apache.parquet.example.data.Group;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class ParquetVectorsReader implements Iterable<float[]> {


  private final Iterable<float[]> compositeIterable;

  public ParquetVectorsReader(List<Path> roots) {

    FlatteningIterable<Path, Path> pathsIterable =
        new FlatteningIterable<>(roots, b -> new PathSorter(b, "*.parquet"));

    Iterable<BoundedRecordReader<Group>> recordReaderIterable =
        new FlatteningIterable<Path, BoundedRecordReader<Group>>(pathsIterable,
            RecordReaderIterable::new);

    Iterable<Group> groupIterable = new FlatteningIterable<BoundedRecordReader<Group>, Group>(
        recordReaderIterable,
        ParquetGroupIterable::new
    );

    Function<Group, float[]> embeddingDecoder = new HFEmbedToFloatAry();

    this.compositeIterable =
        new ConvertingIterable<Group, float[]>(groupIterable, embeddingDecoder);
  }

  @Override
  public Iterator<float[]> iterator() {
    return compositeIterable.iterator();
  }
}
