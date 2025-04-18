package io.nosqlbench.nbvectors.commands.merkle;

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


import java.nio.file.Path;
import java.util.List;

/**
 * Interface for Merkle tree sub-commands.
 * Each implementation handles a specific operation on Merkle trees.
 */
public interface MerkleSubCommand {
    /**
     * Execute the sub-command on the specified files.
     *
     * @param files     The list of files to process
     * @param chunkSize The chunk size to use for Merkle tree operations
     * @param force     Whether to force overwrite of existing files
     * @return true if the operation was successful, false otherwise
     */
    boolean execute(List<Path> files, long chunkSize, boolean force);
}
