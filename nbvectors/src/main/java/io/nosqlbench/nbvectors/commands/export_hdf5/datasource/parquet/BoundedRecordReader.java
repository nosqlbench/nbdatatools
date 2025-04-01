package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

import org.apache.parquet.io.RecordReader;

public record BoundedRecordReader<T>(RecordReader<T> reader, long count) { }
