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
import java.util.stream.Collectors;


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

    // File extensions for merkle tree files
    public static final String MRKL = ".mrkl";
    public static final String MREF = ".mref";
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
     * @param dryrun    Whether to only show what would be done without actually creating files
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
        return command.execute(files, chunkSize, force, dryrun);
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
        public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
            boolean success = true;
            try {
                // Expand directories with extensions
                List<Path> expandedFiles = CMD_merkle.expandDirectoriesWithExtensions(files);

                // Filter out Merkle files (.mrkl and .mref) when creating Merkle files
                expandedFiles = expandedFiles.stream()
                    .filter(path -> {
                        String fileName = path.getFileName().toString().toLowerCase();
                        return !fileName.endsWith(MRKL) && !fileName.endsWith(MREF);
                    })
                    .collect(Collectors.toList());

                if (expandedFiles.isEmpty()) {
                    logger.warn("No files found to process");
                    return true;
                }

                if (dryrun) {
                    logger.info("DRY RUN: Would process {} files", expandedFiles.size());
                } else {
                    logger.info("Processing {} files", expandedFiles.size());
                }

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
                                    if (dryrun) {
                                        logger.info("DRY RUN: Would recreate corrupted Merkle file for: {}", file);
                                    } else {
                                        logger.warn("Merkle file is corrupted and will be recreated: {}", file);
                                    }
                                } else {
                                    // Merkle file is valid, now check if it's up-to-date
                                    long sourceLastModified = Files.getLastModifiedTime(file).toMillis();
                                    long merkleLastModified = Files.getLastModifiedTime(merklePath).toMillis();

                                    if (merkleLastModified >= sourceLastModified) {
                                        // Merkle file is up-to-date, skip this file
                                        if (dryrun) {
                                            logger.info("DRY RUN: Would skip file as Merkle file is up-to-date: {}", file);
                                        } else {
                                            logger.info("Skipping file as Merkle file is up-to-date: {}", file);
                                        }
                                        continue;
                                    } else {
                                        // Merkle file exists but is older than the source file
                                        if (dryrun) {
                                            if (force) {
                                                logger.info("DRY RUN: Would recreate outdated Merkle file for: {}", file);
                                            } else {
                                                logger.error("DRY RUN: Would skip outdated Merkle file for: {} (use --force to overwrite)", file);
                                                success = false;
                                                continue;
                                            }
                                        } else {
                                            logger.error("Merkle file exists but is outdated for: {} (use --force to overwrite)", file);
                                            success = false;
                                            continue;
                                        }
                                    }
                                }
                            } else if (dryrun) {
                                // If force is true and dryrun is true
                                logger.info("DRY RUN: Would overwrite existing Merkle file for: {}", file);
                            } else {
                                // If force is true and dryrun is false
                                logger.info("Overwriting existing Merkle file for: {}", file);
                            }
                        } else if (dryrun) {
                            // No existing Merkle file and dryrun is true
                            logger.info("DRY RUN: Would create new Merkle file for: {}", file);
                            continue;
                        }

                        // Call the existing createMerkleFile method from CMD_merkle if not in dryrun mode
                        if (!dryrun) {
                            CMD_merkle cmd = new CMD_merkle();
                            cmd.createMerkleFile(file, chunkSize);
                        }
                    } catch (Exception e) {
                        logger.error("Error {} Merkle file for: {}", dryrun ? "analyzing" : "creating", file, e);
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
        public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
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

                    if (dryrun) {
                        logger.info("DRY RUN: Would verify file against its Merkle tree: {}", file);
                    } else {
                        // Call the existing verifyFile method from CMD_merkle
                        CMD_merkle cmd = new CMD_merkle();
                        cmd.verifyFile(file, merklePath, chunkSize);
                    }
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
        public boolean execute(List<Path> files, long chunkSize, boolean force, boolean dryrun) {
            boolean success = true;
            for (Path file : files) {
                try {
                    if (!Files.exists(file)) {
                        logger.error("File not found: {}", file);
                        success = false;
                        continue;
                    }

                    // Determine the appropriate Merkle file path based on the file extension
                    Path merklePath = determineMerklePath(file);

                    if (!Files.exists(merklePath)) {
                        logger.error("Merkle file not found for: {}", file);
                        success = false;
                        continue;
                    }

                    if (dryrun) {
                        logger.info("DRY RUN: Would display summary for Merkle file: {}", merklePath);
                    } else {
                        // Call the existing displayMerkleSummary method from CMD_merkle
                        CMD_merkle cmd = new CMD_merkle();
                        cmd.displayMerkleSummary(merklePath);
                    }
                } catch (Exception e) {
                    logger.error("Error displaying summary for: {}", file, e);
                    success = false;
                }
            }
            return success;
        }

        /**
         * Determines the appropriate Merkle file path based on the file extension.
         *
         * @param file The input file path
         * @return The path to the Merkle file
         */
        private Path determineMerklePath(Path file) {
            String fileName = file.getFileName().toString();

            // If the file is already a Merkle file (.mrkl or .mref), use it directly
            if (fileName.endsWith(MRKL) || fileName.endsWith(MREF)) {
                return file;
            }

            // Otherwise, look for an associated Merkle file
            Path merklePath = file.resolveSibling(fileName + MRKL);
            if (Files.exists(merklePath)) {
                return merklePath;
            }

            // If .mrkl doesn't exist, try .mref
            Path mrefPath = file.resolveSibling(fileName + MREF);
            if (Files.exists(mrefPath)) {
                return mrefPath;
            }

            // Default to .mrkl if neither exists
            return merklePath;
        }
    }
}
