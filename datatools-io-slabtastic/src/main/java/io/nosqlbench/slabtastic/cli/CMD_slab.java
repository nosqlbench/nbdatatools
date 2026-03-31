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

package io.nosqlbench.slabtastic.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

/// Top-level `slab` command for slabtastic file maintenance.
///
/// Provides subcommands for analyzing, validating, getting records, rewriting,
/// appending, importing, and exporting slabtastic files.
@CommandLine.Command(
    name = "slab",
    header = "Slabtastic file maintenance tool",
    description = "Analyze, validate, get, rewrite, and manage slabtastic files.",
    subcommands = {
        CMD_slab_analyze.class,
        CMD_slab_check.class,
        CMD_slab_get.class,
        CMD_slab_rewrite.class,
        CMD_slab_append.class,
        CMD_slab_import.class,
        CMD_slab_export.class,
        CMD_slab_explain.class,
        CMD_slab_namespaces.class,
        CommandLine.HelpCommand.class
    }
)
public class CMD_slab implements Callable<Integer> {

    /// Creates a new instance of the slab command.
    public CMD_slab() {}

    /// Entry point for standalone execution.
    ///
    /// @param args command-line arguments
    public static void main(String[] args) {
        int exitCode = new CommandLine(new CMD_slab())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }
}
