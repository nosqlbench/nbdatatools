package io.nosqlbench.nbvectors.commands.merkle;

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
