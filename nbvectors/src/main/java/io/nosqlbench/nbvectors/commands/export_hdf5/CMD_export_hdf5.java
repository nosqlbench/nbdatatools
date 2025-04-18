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


import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.BasicTestDataSource;
import io.nosqlbench.nbvectors.commands.build_hdf5.datasource.ObjectLoader;
import io.nosqlbench.nbvectors.commands.build_hdf5.writers.KnnDataWriter;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import picocli.CommandLine;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "export_hdf5",
    headerHeading = "Usage:%n%n",
    header = "export HDF5 KNN answer-keys from other formats",
    description = """
        Reads base vectors, query vectors, neighborhood indices, and optionally, neighbor distances,
        from ivec, fvec and other formats, and creates HDF5 vector test data files.
        
        When no mapping file is provided, then these individual input files are specified by the respective options.
        The output file name is determined, by default, from a template that uses common parameter tokens so that
        files are automatically named and unique as long as the parameter sets are unique.
        
        When a mapping file is provided, then the mapping file is used to determine the input files and output file names.
        This is an example of the mapping file format:
        
            # mapping.yaml
            filekey: #this is only unique within the mapping file and is not used otherwise
             # the following are required
             query: my_query_vectors.fvec
             base: my_base_vectors.fvec
             indices: my_indices.ivec
             url: https://...
             license: The license of the model used
             vendor: The vendor of the model used
             model: The name of the model used
            # the following are optional
             distance_function: The distance function used # defaults to COSINE
             distances: my_distances.ivec # this is only added to the hdf5 file if provided
             notes: Any notes about the model used
        
        When a mapping file is not provided, the remaining metadata can be provided in a yaml
        file and specified with the --metadata option.
        
        """,
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_export_hdf5 implements Callable<Integer> {
  private final static String DEFAULT_TEMPLATE =
      "[model][_d{dims*}][_b{vectors*}][_q{queries*}][_i{indices*}][_mk{max_k*}].hdf5";

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "[model][_d{dims*}][_b{vectors*}][_q{queries*}][_i{indices*}][_mk{max_k*}]"
                     + ".hdf5",
      description = "The HDF5 file to write\ndefault: ${DEFAULT-VALUE}")
  private String outfile;

  @Option(names = {"-m", "--mapping-file"},
      required = false,
      description = "The mapping file(s) to read. If provided, exports multiple files according "
                    + "to contents of the mapping files.")
  private List<Path> mappingFiles;

  @CommandLine.Option(names = {"--query_vectors", "--query"},
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

  @CommandLine.Option(names = {"--neighbor_indices", "--indices"},
      required = false,
      description = "The query_neighbors file to read")
  private Path neighbors;

  @CommandLine.Option(names = {"--neighbor_distances", "--distances"},
      required = false,
      description = "The query_distances file to read")
  private Path distances;

  @CommandLine.Option(names = {"--base_vectors", "--base"},
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

  @CommandLine.Option(names = {"--layout"},
      required = false,
      description = "The layout file to read")
  private Path layout;

  @CommandLine.Option(names = {"--force"},
      description = "Force overwrite of existing HDF5 files,"
                    + " even if no changes to mapping since last export")
  private boolean force = false;

  /// run an export_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
//    System.setProperty("slf4j.internal.verbosity", "ERROR");
//    System.setProperty(
//        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
//        CustomConfigurationFactory.class.getCanonicalName()
//    );

    CMD_export_hdf5 command = new CMD_export_hdf5();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    List<BatchConfig> configs = new ArrayList<>();
    if (this.mappingFiles != null && !mappingFiles.isEmpty()) {
      for (Path mappingFile : this.mappingFiles) {
        configs.add(BatchConfig.file(mappingFile));
      }
    } else {
      if (metadataFile == null) {

        throw new RuntimeException("metadata file is required when not using a mapping file");
      }
      RootGroupAttributes rga = ObjectLoader.load(metadataFile, RootGroupAttributes::fromMap).orElseThrow(
          () -> new RuntimeException("root group attributes failed to load from " + metadataFile));

      DataGroupConfig cfg = new DataGroupConfig(
          Optional.ofNullable(this.base_vectors),
          Optional.ofNullable(this.query_vectors),
          Optional.ofNullable(this.neighbors),
          Optional.ofNullable(this.distances),
          Optional.ofNullable(this.base_content),
          Optional.ofNullable(this.query_terms),
          Optional.ofNullable(this.query_filters),
          Optional.ofNullable(this.layout),
          rga
      );
      configs.add(new BatchConfig(Map.of("from CLI options:", cfg), Instant.now()));
    }

    String template = this.outfile;
    if (outfile.contains("TEMPLATE")) {
      outfile = outfile.replace("TEMPLATE", DEFAULT_TEMPLATE);
    }
    for (BatchConfig config : configs) {
      for (String entry : config.files().keySet()) {
        System.err.println("exporting " + entry);
        DataGroupConfig vectorFilesConfig = config.files().get(entry);

        if (!this.force && config.epochTimestamp()
            .isBefore(vectorFilesConfig.getLastModifiedTime()))
        {
          System.err.println(
              "skipping " + entry + " because it is up to date (use --force to override)");
          continue;
        }
        BasicTestDataSource source = new BasicTestDataSource(vectorFilesConfig);
        KnnDataWriter writer = new KnnDataWriter(this.outfile, source);

        if (vectorFilesConfig.layout().isPresent()) {
          throw new RuntimeException("use export_hdf5new instead of export_hdf5 for layouts for "
                                     + "now");
        } else {
          writer.writeHdf5();
        }

      }
    }
    return 0;
  }
}


