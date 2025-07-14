package io.nosqlbench.command.merkle.subcommands;

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

import io.nosqlbench.vectordata.merkle.MerkleTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Command to display the path from a leaf node to the root for a given chunk index.
 */
@Command(
    name = "path",
    description = "Display the hash path from a leaf node to the root for a given chunk index"
)
public class CMD_merkle_path implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_path.class);

    // File extensions for merkle tree files
    public static final String MRKL = ".mrkl";
    public static final String MREF = ".mref";

    @Parameters(index = "0", description = "File to process")
    private Path file;

    @Parameters(index = "1", description = "Chunk index to query")
    private int chunkIndex;

    @Override
    public Integer call() throws Exception {
        boolean success = execute(file, chunkIndex);
        return success ? 0 : 1;
    }

    /**
     * Execute the path command on the specified file and chunk index.
     * This implementation uses sparse random access to read only the necessary nodes
     * from the merkle tree file, avoiding loading the entire tree into memory.
     *
     * @param file       The file to process
     * @param chunkIndex The chunk index to query
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(Path file, int chunkIndex) {
        try {
            if (!Files.exists(file)) {
                logger.error("File not found: {}", file);
                return false;
            }

            // Determine the appropriate Merkle file path based on the file extension
            Path merklePath = determineMerklePath(file);

            if (!Files.exists(merklePath)) {
                logger.error("Merkle file not found for: {}", file);
                return false;
            }

            // Load the MerkleTree from the file
            // Then get the path from the leaf to the root
            MerkleTree merkleTree = MerkleTree.load(merklePath);
            List<byte[]> path = merkleTree.getPathToRoot(chunkIndex);

            // Display the path
            System.out.println("Hash path from leaf node " + chunkIndex + " to root:");
            System.out.println("---------------------------------------------------");

            for (int i = 0; i < path.size(); i++) {
                byte[] hash = path.get(i);
                String level = (i == 0) ? "Leaf " + chunkIndex : 
                               (i == path.size() - 1) ? "Root" : 
                               "Level " + i;

                System.out.println(level + ": " + bytesToHex(hash));
            }

            return true;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid chunk index: {}. {}", chunkIndex, e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("Error displaying path for chunk index {} in file {}: {}", 
                chunkIndex, file, e.getMessage());
            return false;
        }
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

    /**
     * Converts a byte array to a hex string.
     *
     * @param bytes The byte array to convert
     * @return A hex string representation
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
