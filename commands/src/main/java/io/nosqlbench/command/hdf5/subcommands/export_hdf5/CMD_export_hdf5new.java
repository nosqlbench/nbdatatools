package io.nosqlbench.command.hdf5.subcommands.export_hdf5;

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


import io.nosqlbench.command.hdf5.subcommands.build_hdf5.writers.KnnDataProfilesWriter;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "export_hdf5new",
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
public class CMD_export_hdf5new implements Callable<Integer> {


  @Option(names = {"-o", "--outfile"},
      defaultValue = "[model][_d{dims*}][_b{vectors*}][_q{queries*}][_i{indices*}][_mk{max_k*}]"
                     + ".hdf5",
      description = "The HDF5 file to write\ndefault: ${DEFAULT-VALUE}")
  private String outfile;

  @Option(names = {"--layout"}, required = false, description = "The layout file to read")
  private List<Path> layouts = new ArrayList<>();

  @Option(names = {"--force"},
      description = "Force overwrite of existing HDF5 files,"
                    + " even if no changes to mapping since last export")
  private boolean force = false;

  /// Create the default CMD_export_hdf5new command
  public CMD_export_hdf5new() {}

  /// run an export_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    //    System.setProperty("slf4j.internal.verbosity", "ERROR");
    //    System.setProperty(
    //        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
    //        CustomConfigurationFactory.class.getCanonicalName()
    //    );

    CMD_export_hdf5new command = new CMD_export_hdf5new();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    List<TestGroupLayout> configs = new ArrayList<>();
    layouts.stream().map(TestGroupLayout::load).forEach(configs::add);

    //    if (this.mappingFiles != null && !mappingFiles.isEmpty()) {
    //      for (Path mappingFile : this.mappingFiles) {
    //        configs.add(GroupedDataConfig.file(mappingFile));
    //      }
    //    } else {
    //      throw new RuntimeException("mapping file is required");
    //    }
    //
    //    String template = this.outfile;

    for (TestGroupLayout config : configs) {

      //      if (!this.force && config.epochTimestamp()
      //          .isBefore(vectorFilesConfig.getLastModifiedTime()))
      //      {
      //        System.err.println(
      //            "skipping " + entry + " because it is up to date (use --force to override)");
      //        continue;
      //      }

      KnnDataProfilesWriter writer = new KnnDataProfilesWriter(this.outfile, config);
      writer.writeHdf5();
    }
    return 0;
  }
}
