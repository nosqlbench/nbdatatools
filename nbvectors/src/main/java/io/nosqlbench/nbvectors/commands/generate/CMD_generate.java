package io.nosqlbench.nbvectors.commands.generate;

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


import io.nosqlbench.nbvectors.commands.generate.commands.IvecShuffle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/**
 * Alternative implementation of the catalog command, building hierarchical catalogs
 * in a simpler, declarative pass over all entries.
 */
@CommandLine.Command(name = "generate",
    header = "Generate (deterministically) orderings, etc for test data prep",
    description = """
        This provides a set of basic procedural generation utilities for
        the purposes of preparing test data
        """,
    exitCodeList = {"0: success", "1: warning", "2: error"},
subcommands = {IvecShuffle.class})
public class CMD_generate {
    private static final Logger logger = LogManager.getLogger(CMD_generate.class);

    @CommandLine.Parameters(description = "commands")
    private List<String> commands = new ArrayList<>();

    public static void main(String[] args) {
        CMD_generate cmd = new CMD_generate();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

}
