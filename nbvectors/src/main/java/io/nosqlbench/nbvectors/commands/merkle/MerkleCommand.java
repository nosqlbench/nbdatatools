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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;


/**
 * Enum representing the available Merkle tree commands.
 * Each enum value has an associated implementation of the MerkleSubCommand interface.
 */
public enum MerkleCommand {
    /**
     * Create a new Merkle tree file for each input file.
     */
    CREATE("create", "Create new Merkle tree files", new CreateCommand()),

    /**
     * Verify files against their existing Merkle tree files.
     */
    VERIFY("verify", "Verify files against their Merkle trees", new VerifyCommand()),

    /**
     * Display summary information about existing Merkle tree files.
     */
    SUMMARY("summary", "Display summary information about Merkle trees", new SummaryCommand());

    // File extension for merkle tree files
    public static final String MRKL = ".mrkl";
    private static final Logger logger = LogManager.getLogger(MerkleCommand.class);
    private final String name;
    private final String description;
    private final MerkleSubCommand command;

    MerkleCommand(String name, String description, MerkleSubCommand command) {
        this.name = name;
        this.description = description;
        this.command = command;
    }

    /**
     * Get the name of the command.
     *
     * @return The command name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the description of the command.
     *
     * @return The command description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Execute the command on the specified files.
     *
     * @param files     The list of files to process
     * @param chunkSize The chunk size to use for Merkle tree operations
     * @param force     Whether to force overwrite of existing files
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(List<Path> files, long chunkSize, boolean force) {
        return command.execute(files, chunkSize, force);
    }

    /**
     * Find a command by name.
     *
     * @param name The name of the command to find
     * @return The command, or null if not found
     */
    public static MerkleCommand findByName(String name) {
        for (MerkleCommand command : values()) {
            if (command.getName().equalsIgnoreCase(name)) {
                return command;
            }
        }
        return null;
    }

    /**
     * Implementation of the CREATE command.
     */
    private static class CreateCommand implements MerkleSubCommand {
        @Override
        public boolean execute(List<Path> files, long chunkSize, boolean force) {
            boolean success = true;
            try {
                // Expand directories with extensions
                List<Path> expandedFiles = CMD_merkle.expandDirectoriesWithExtensions(files);

                if (expandedFiles.isEmpty()) {
                    logger.warn("No files found to process");
                    return true;
                }

                logger.info("Processing {} files", expandedFiles.size());

                for (Path file : expandedFiles) {
                    try {
                        if (!Files.exists(file) || !Files.isRegularFile(file)) {
                            logger.error("File not found or not a regular file: {}", file);
                            success = false;
                            continue;
                        }

                        Path merklePath = file.resolveSibling(file.getFileName() + MRKL);
                        if (Files.exists(merklePath)) {
                            if (!force) {
                                // First verify the integrity of the existing Merkle file
                                CMD_merkle cmd = new CMD_merkle();
                                boolean isValid = cmd.verifyMerkleFileIntegrity(merklePath);

                                if (!isValid) {
                                    // Merkle file is corrupted, we need to recreate it
                                    logger.warn("Merkle file is corrupted and will be recreated: {}", file);
                                } else {
                                    // Merkle file is valid, now check if it's up-to-date
                                    long sourceLastModified = Files.getLastModifiedTime(file).toMillis();
                                    long merkleLastModified = Files.getLastModifiedTime(merklePath).toMillis();

                                    if (merkleLastModified >= sourceLastModified) {
                                        // Merkle file is up-to-date, skip this file
                                        logger.info("Skipping file as Merkle file is up-to-date: {}", file);
                                        continue;
                                    } else {
                                        // Merkle file exists but is older than the source file
                                        logger.error("Merkle file exists but is outdated for: {} (use --force to overwrite)", file);
                                        success = false;
                                        continue;
                                    }
                                }
                            }
                            // If force is true or the Merkle file is corrupted, we'll proceed to recreate it
                            logger.info("Overwriting existing Merkle file for: {}", file);
                        }

                        // Call the existing createMerkleFile method from CMD_merkle
                        CMD_merkle cmd = new CMD_merkle();
                        cmd.createMerkleFile(file, chunkSize);
                    } catch (Exception e) {
                        logger.error("Error creating Merkle file for: {}", file, e);
                        success = false;
                    }
                }
            } catch (IOException e) {
                logger.error("Error expanding directories: {}", e.getMessage(), e);
                success = false;
            }
            return success;
        }
    }

    /**
     * Implementation of the VERIFY command.
     */
    private static class VerifyCommand implements MerkleSubCommand {
        @Override
        public boolean execute(List<Path> files, long chunkSize, boolean force) {
            boolean success = true;
            for (Path file : files) {
                try {
                    if (!Files.exists(file)) {
                        logger.error("File not found: {}", file);
                        success = false;
                        continue;
                    }

                    Path merklePath = file.resolveSibling(file.getFileName() + MRKL);
                    if (!Files.exists(merklePath)) {
                        logger.error("Merkle file not found for: {}", file);
                        success = false;
                        continue;
                    }

                    // Call the existing verifyFile method from CMD_merkle
                    CMD_merkle cmd = new CMD_merkle();
                    cmd.verifyFile(file, merklePath, chunkSize);
                } catch (Exception e) {
                    logger.error("Error verifying file: {}", file, e);
                    success = false;
                }
            }
            return success;
        }
    }

    /**
     * Implementation of the SUMMARY command.
     */
    private static class SummaryCommand implements MerkleSubCommand {
        @Override
        public boolean execute(List<Path> files, long chunkSize, boolean force) {
            boolean success = true;
            for (Path file : files) {
                try {
                    if (!Files.exists(file)) {
                        logger.error("File not found: {}", file);
                        success = false;
                        continue;
                    }

                    Path merklePath = file.resolveSibling(file.getFileName() + MRKL);
                    if (!Files.exists(merklePath)) {
                        logger.error("Merkle file not found for: {}", file);
                        success = false;
                        continue;
                    }

                    // Call the existing displayMerkleSummary method from CMD_merkle
                    CMD_merkle cmd = new CMD_merkle();
                    cmd.displayMerkleSummary(merklePath);
                } catch (Exception e) {
                    logger.error("Error displaying summary for: {}", file, e);
                    success = false;
                }
            }
            return success;
        }
    }
}
