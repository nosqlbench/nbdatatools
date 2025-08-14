package io.nosqlbench.command.analyze.subcommands;

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

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;

/// Select a specific vector from a vector file by its ordinal position
/// 
/// This command opens a file of any supported encoding and retrieves the vector
/// at the specified ordinal position (0-based index).
@CommandLine.Command(name = "select",
    header = "Select a specific vector from a vector file",
    description = "Retrieves and displays a vector at the specified ordinal position from a vector file",
    exitCodeList = {"0: success", "1: error processing file", "2: invalid ordinal"})
public class CMD_analyze_select implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_select.class);

    @CommandLine.Parameters(index = "0", description = "File to read from")
    private Path file;

    @CommandLine.Parameters(index = "1", description = "Ordinal position of the vector to select (0-based)")
    private int ordinal;

    /// Execute the command to select a vector from the specified file
    /// 
    /// @return 0 for success, 1 for error processing file, 2 for invalid ordinal
    @Override
    public Integer call() {
        try {
            if (!Files.exists(file)) {
                String errorMsg = "File not found: " + file;
                logger.error(errorMsg);
                System.err.println(errorMsg);
                return 1;
            }

            String fileExtension = getFileExtension(file);
            System.out.printf("Selecting vector at position %d from file: %s%n", ordinal, file);

            try {
                // Determine file type based on extension
                FileType fileType;
                Class<?> dataType;

                switch (fileExtension.toLowerCase()) {
                    case "fvec", "fvecs" -> {
                        fileType = FileType.xvec;
                        dataType = float[].class;
                        return selectVector(file, dataType, fileType);
                    }
                    case "ivec", "ivecs" -> {
                        fileType = FileType.xvec;
                        dataType = int[].class;
                        return selectVector(file, dataType, fileType);
                    }
                    case "parquet" -> {
                        fileType = FileType.parquet;
                        dataType = float[].class;
                        return selectVector(file, dataType, fileType);
                    }
                    case "csv" -> {
                        fileType = FileType.csv;
                        dataType = float[].class;
                        return selectVector(file, dataType, fileType);
                    }
                    default -> {
                        logger.error("Unsupported file type: {}", fileExtension);
                        return 1;
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing file {}: {}", file, e.getMessage());
                return 1;
            }
        } catch (Exception e) {
            logger.error("Error processing file", e);
            return 1;
        }
    }

    /// Get the file extension from a path
    /// 
    /// @param file The file path
    /// @return The file extension (without the dot)
    private String getFileExtension(Path file) {
        String fileName = file.getFileName().toString();
        int lastDotIndex = fileName.lastIndexOf('.');
        return lastDotIndex > 0 ? fileName.substring(lastDotIndex + 1) : "";
    }

    /// Select a vector from a file at the specified ordinal position
    /// 
    /// @param file The file to process
    /// @param dataType The class representing the data type (float[].class or int[].class)
    /// @param fileType The file type (FileType.xvec, FileType.parquet, etc.)
    /// @return 0 for success, 1 for error, 2 for invalid ordinal
    private <T> Integer selectVector(Path file, Class<T> dataType, FileType fileType) {
        try {
            // Open the file using VectorFileIO
            VectorFileArray<T> vectorArray = VectorFileIO.randomAccess(fileType, dataType, file);
            int vectorCount = vectorArray.getSize();

            // Check if the ordinal is valid
            if (ordinal < 0 || ordinal >= vectorCount) {
                String errorMsg = String.format("Invalid ordinal: %d. Valid range is 0 to %d", ordinal, vectorCount - 1);
                logger.error(errorMsg);
                System.err.println(errorMsg);
                vectorArray.close();
                return 2;
            }

            // Get the vector at the specified ordinal
            T vector = vectorArray.get(ordinal);

            // Print the vector data
            System.out.println("Vector Data:");
            if (vector instanceof float[] floatVector) {
                System.out.printf("- Type: float[%d]%n", floatVector.length);
                System.out.println("- Values: " + Arrays.toString(floatVector));
            } else if (vector instanceof int[] intVector) {
                System.out.printf("- Type: int[%d]%n", intVector.length);
                System.out.println("- Values: " + Arrays.toString(intVector));
            } else {
                System.out.printf("- Type: %s%n", vector.getClass().getSimpleName());
                System.out.println("- Values: " + vector);
            }

            // Close the vector array
            vectorArray.close();
            return 0;
        } catch (Exception e) {
            logger.error("Error selecting vector from file {}: {}", file, e.getMessage());
            throw new RuntimeException("Failed to select vector: " + e.getMessage(), e);
        }
    }
}
