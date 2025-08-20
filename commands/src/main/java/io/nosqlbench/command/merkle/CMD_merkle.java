package io.nosqlbench.command.merkle;

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

import io.nosqlbench.command.merkle.subcommands.CMD_merkle_create;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_diff;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_path;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_spoilbits;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_spoilchunks;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_summary;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_treeview;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_verify;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

/// Command to create, verify, and manage Merkle tree files for data integrity.
/// This class serves as the entry point for all Merkle tree operations.
/// Implementation details are delegated to the appropriate subcommand classes.
@Command(name = "merkle",
    header = "commands for managing merkle tree files",
    description = """
        The commands in this section allow you to create merkle trees which optimize downloading
        with the vectordata module. It will use these remote trees as a way to incrementally
        pull down data as it is read and verify checksums for the required sections only.""",
    subcommands = {
        CMD_merkle_create.class,
        CMD_merkle_verify.class,
        CMD_merkle_summary.class,
        CMD_merkle_path.class,
        CMD_merkle_diff.class,
        CMD_merkle_treeview.class,
        CMD_merkle_spoilbits.class,
        CMD_merkle_spoilchunks.class,
        HelpCommand.class
    })
public class CMD_merkle implements BundledCommand {
}
