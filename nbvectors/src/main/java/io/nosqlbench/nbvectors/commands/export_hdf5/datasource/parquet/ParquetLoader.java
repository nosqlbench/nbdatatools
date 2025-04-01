package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.spec.access.datasets.types.BaseVectors;
import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;
import io.nosqlbench.nbvectors.spec.views.SpecDataSource;

import java.util.Optional;

public class ParquetLoader implements SpecDataSource {

  private final ParquetLoaderConfig config;

  public ParquetLoader(ParquetLoaderConfig config) {
    this.config = config;
  }

  @Override
  public Optional<Iterable<float[]>> getBaseVectors() {
    return config.getBaseVectorsLayout().map(ParquetVectorsReader::new);
  }

  @Override
  public Optional<Iterable<?>> getBaseContent() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<LongIndexedFloatVector>> getQueryVectors() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<?>> getQueryTerms() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<PNode<?>>> getQueryFilters() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<int[]>> getNeighborIndices() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<float[]>> getNeighborDistances() {
    return Optional.empty();
  }

  @Override
  public RootGroupAttributes getMetadata() {
    return null;
  }
}
