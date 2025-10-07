package io.nosqlbench.command.convert;

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

import io.nosqlbench.command.convert.subcommands.CMD_convert_file;
import io.nosqlbench.command.convert.subcommands.CMD_convert_hdf52dataset;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 # Vector Format Conversion Tool

 This command provides utilities for converting vector data between different formats:

 ## Subcommands
 - `file`: Convert between different vector file formats (fvec, ivec, bvec, csv, json)
 - `hdf52dataset`: Convert HDF5 datasets to vectordata dataset format

 ## Supported Formats
 - fvec: float vector format (4-byte float values)
 - ivec: integer vector format (4-byte integer values)
 - bvec: binary vector format (1-byte unsigned values)
 - csv: comma-separated text format
 - json: JSON-based vector format
 - HDF5: Hierarchical Data Format version 5
 - vectordata dataset: Directory-based dataset format with dataset.yaml metadata

 # Basic Usage
 ```
 convert file --input vectors.fvec --output vectors.csv
 convert hdf52dataset --input dataset.hdf5 --output ./my_dataset/
 ```
 */
@Selector("convert")
@CommandLine.Command(name = "convert",
    header = "Convert between different vector data formats",
    description = "This provides utilities for converting vector data between different formats.\n" +
        "Use subcommands to convert between specific format types.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0: success", "1: warning", "2: error"},
    subcommands = {
        CMD_convert_file.class,
        CMD_convert_hdf52dataset.class,
        CommandLine.HelpCommand.class
    })
public class CMD_convert implements Callable<Integer>, BundledCommand {
    private static final Logger logger = LogManager.getLogger(CMD_convert.class);

    /**
     * Callback interface for conversion progress and completion.
     * Used by SingleFileConverter for progress reporting.
     */
    public interface ConversionCallback {
        void onSuccess(Path inputFile, Path outputFile, int vectorsProcessed);
        void onError(String message);
    }

    /**
     * Create the default CMD_convert command
     */
    public CMD_convert() {
    }

    /**
     * Run a convert command
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        CMD_convert cmd = new CMD_convert();
        int exitCode = new CommandLine(cmd)
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        // Print help information if no subcommand is specified
        CommandLine.usage(this, System.out);
        return 0;
    }
}