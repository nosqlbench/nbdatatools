/// Parquet reader module
module parquet.reader {
  requires testdata.apis;
  requires hadoop.common;
  requires hadoop.mapreduce.client.core;
  requires parquet.common;
  requires parquet.column;
  requires parquet.hadoop;

  // ParquetVectorsReader cannot be provided as a service implementation
  // because it doesn't have a public default constructor
  // provides io.nosqlbench.nbvectors.api.fileio.SizedVectorStreamReader with
  //   io.nosqlbench.nbvectors.datasource.parquet.ParquetVectorsReader;
}
