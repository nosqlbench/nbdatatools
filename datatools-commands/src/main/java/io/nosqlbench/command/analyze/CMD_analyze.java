package io.nosqlbench.command.analyze;

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

import io.nosqlbench.command.analyze.subcommands.CMD_analyze_describe;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_check_endian;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_find;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_select;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_slice;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_verifyknn;
import io.nosqlbench.command.analyze.subcommands.CMD_analyze_zeros;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/// The analyze command contains several subcommands to help understand the contents of test data files
/// 
/// This is an umbrella command for various analysis subcommands.
@CommandLine.Command(name = "analyze",
    header = "Analyze vector test data files",
    description = "Contains subcommands to help understand the contents of test data files",
    subcommands = {
        CMD_analyze_zeros.class,
        CMD_analyze_verifyknn.class,
        CMD_analyze_describe.class,
        CMD_analyze_select.class,
        CMD_analyze_check_endian.class,
        CMD_analyze_find.class,
        CMD_analyze_slice.class
    })
public class CMD_analyze implements Callable<Integer>, BundledCommand {
    private static final Logger logger = LogManager.getLogger(CMD_analyze.class);

    /// Run CMD_analyze
    /// 
    /// @param args Command line arguments
    public static void main(String[] args) {
        System.exit(new CommandLine(new CMD_analyze()).execute(args));
    }

    /// Execute the analyze command
    /// 
    /// @return 0 for success, 1 for error
    @Override
    public Integer call() {
        // Print help information if no subcommand is specified
        CommandLine.usage(this, System.out);
        return 0;
    }

}
