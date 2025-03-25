package io.nosqlbench.nbvectors.commands.export_hdf5;

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


import io.nosqlbench.nbvectors.commands.build_hdf5.BasicTestDataSource;
import io.nosqlbench.nbvectors.commands.build_hdf5.KnnDataWriter;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "export_hdf5", description = "export HDF5 KNN answer-keys from other formats")
public class CMD_export_hdf5 implements Callable<Integer> {

  private static String DEFAULT_TEMPLATE =
      "[model][_{variant*}][_d{dims}][_b{vectors}][_q{queries" + "}][_mk{max_k}].hdf5";

//  @CommandLine.ArgGroup(exclusive = true)
//  Outfile outfile;

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "out.hdf5",
      description = "The " + "HDF5" + " file to " + "write")
  private String outfile;

//  static class Outfile {
//    @Option(names = {"-t", "--outfile-template"},
//        required = true,
//        defaultValue = "[model][_{variant*}][_d{dims}][_b{vectors}][_q{queries}][_mk{max_k}].hdf5",
//        description = "The HDF5 file template to write to")
//    private Path template;
//  }
//
  @Option(names = {"-m", "--mapping-file"},
      required = true,
      description = "The mapping file to read")
  private Path mappingFile;

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

  @CommandLine.Option(names = {"--neighbor_indices"},
      required = false,
      description = "The query_neighbors file to read")
  private Path neighbors;

  @CommandLine.Option(names = {"--neighbor_distances"},
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

  /// run an export_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    CMD_export_hdf5 command = new CMD_export_hdf5();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (this.mappingFile != null) {
      BatchConfig cfg = BatchConfig.file(this.mappingFile);

      String template = this.outfile;
      if (outfile.contains("TEMPLATE")) {
        outfile = outfile.replace("TEMPLATE", DEFAULT_TEMPLATE);
      }

      for (String entry : cfg.files().keySet()) {
        System.err.println("exporting " + entry);
        VectorFilesConfig vectorFilesConfig = cfg.files().get(entry);
        BasicTestDataSource source = new BasicTestDataSource(vectorFilesConfig);
        KnnDataWriter writer = new KnnDataWriter(this.outfile, source);
        writer.writeHdf5();
      }

      return 0;
    } else {
      RootGroupAttributes rga = RootGroupAttributes.fromFile(metadataFile);

      VectorFilesConfig cfg = new VectorFilesConfig(
          this.base_vectors,
          this.query_vectors,
          this.neighbors,
          Optional.ofNullable(this.distances),
          Optional.ofNullable(this.base_content),
          Optional.ofNullable(this.query_terms),
          Optional.ofNullable(this.query_filters),
          rga
      );

      BasicTestDataSource source = new BasicTestDataSource(cfg);
      KnnDataWriter writer = new KnnDataWriter(this.outfile, source);
      writer.writeHdf5();
      return 0;

    }


  }
}


