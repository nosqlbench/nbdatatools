package io.nosqlbench.nbvectors.commands.batch_export;

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
import io.nosqlbench.nbvectors.commands.export_hdf5.VectorFilesConfig;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "batch_export",
    description = "batch export HDF5 KNN answer-keys from other " + "formats, wrapping export_hdf5")
public class CMD_batch_export implements Callable<Integer> {

  private static String DEFAULT_TEMPLATE =
      "[model][_{variant*}][_d{dims}][_b{vectors}][_q{queries" + "}][_mk{max_k}].hdf5";

  @Option(names = {"-t", "--outfile-template"},
      required = true,
      defaultValue = "[model][_{variant*}][_d{dims}][_b{vectors}][_q{queries}][_mk{max_k}].hdf5",
      description = "The HDF5 file template to write to")
  private Path hdfOutTemplate;

  @Option(names = {"-m", "--mapping-file"},
      required = true,
      description = "The mapping file to read")
  private Path mappingFile;

  /// run an export_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    CMD_batch_export command = new CMD_batch_export();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    BatchConfig cfg = BatchConfig.file(this.mappingFile);

    String template = this.hdfOutTemplate.toString();
    if (template.contains("TEMPLATE")) {
      template = template.replace("TEMPLATE", DEFAULT_TEMPLATE);
      this.hdfOutTemplate = Path.of(template);
    }

    for (String entry : cfg.files().keySet()) {
      System.err.println("exporting " + entry);
      VectorFilesConfig vectorFilesConfig = cfg.files().get(entry);
      BasicTestDataSource source = new BasicTestDataSource(vectorFilesConfig);
      KnnDataWriter writer = new KnnDataWriter(this.hdfOutTemplate, source);
      writer.writeHdf5();
    }

    return 0;
  }
}


