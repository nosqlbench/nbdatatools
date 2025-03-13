package io.nosqlbench.nbvectors.importhdf5;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import io.nosqlbench.nbvectors.buildhdf5.BasicTestDataSource;
import io.nosqlbench.nbvectors.spec.attributes.SpecAttributes;
import io.nosqlbench.nbvectors.buildhdf5.KnnDataWriter;
import io.nosqlbench.nbvectors.verifyknn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "importhdf5")
public class CMD_importhdf5 implements Callable<Integer> {

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "out.hdf5",
      description = "The " + "HDF5" + " file to " + "write")
  private Path hdfOutPath;

  @CommandLine.Option(names = {"--query_vectors"},
      required = false,
      description = "The query_vectors file to read")
  private Path query_vectors;

  @CommandLine.Option(names = {"--query_terms"},
      required = false,
      description = "The query_terms file to read")
  private Path query_terms;

  @CommandLine.Option(names = {"--query_filters"},
      required = false,
      description = "The query_filters file to read")
  private Path query_filters;

  @CommandLine.Option(names = {"--neighbors"},
      required = false,
      description = "The query_neighbors file to read")
  private Path neighbors;

  @CommandLine.Option(names = {"--distances"},
      required = false,
      description = "The query_distances file to read")
  private Path distances;

  @CommandLine.Parameters(description = "The files to import and export")
  private List<Path> files;

  @CommandLine.Option(names = {"--base_vectors"},
      required = false,
      description = "The base_vectors file to read")
  private Path base_vectors;

  @CommandLine.Option(names = {"--base_content"},
      required = false,
      description = "The base_content file to read")
  private Path base_content;

  @CommandLine.Option(names = {"--metadata"},
      required = false,
      description = "The metadata file to read")
  private Path metadataFile;

  /// run an importhdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    CMD_importhdf5 command = new CMD_importhdf5();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {

    BasicTestDataSource source = new BasicTestDataSource();
    if (this.base_vectors!=null) {
      source.setBaseVectorsIterator(new FvecToIndexedFloatVector(this.base_vectors).iterator());
    }
    if (this.query_vectors!=null) {
      source.setQueryVectorsIter(new FvecToIndexedFloatVector(this.query_vectors).iterator());
    }
    if (this.neighbors!=null) {
      source.setNeighborIndicesIter(new IvecToIntArray(this.neighbors).iterator());
    }
    if (this.distances!=null) {
      source.setNeighborDistancesIter(new FvecToFloatArray(this.distances).iterator());
    }
    if (this.metadataFile != null) {
      source.setMetadata(SpecAttributes.fromFile(metadataFile));
    } else {
      throw new RuntimeException("metadata file is required");
    }

    try (KnnDataWriter writer = new KnnDataWriter(this.hdfOutPath, source)) {
      writer.writeHdf5();
    }

    return 0;

  }
}


