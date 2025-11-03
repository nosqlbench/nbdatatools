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

import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

/// Describe the contents of a vector file
/// 
/// This command opens a file of any supported encoding and provides information about
/// the data it contains, including what kind of data it has, the dimensions, and
/// how many vectors it contains.
@CommandLine.Command(name = "describe",
    header = "Describe the contents of a vector file",
    description = "Provides information about the data in a vector file, including dimensions and vector count",
    exitCodeList = {"0: success", "1: error processing file"})
public class CMD_analyze_describe implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_describe.class);

    @CommandLine.Parameters(description = "File to describe", arity = "1")
    private Path file;

    /// Execute the command to describe the specified file
    /// 
    /// @return 0 for success, 1 for error
    @Override
    public Integer call() {
        try {
            if (!Files.exists(file)) {
                logger.error("File not found: {}", file);
                return 1;
            }

            String fileExtension = getFileExtension(file);
            System.out.printf("Analyzing file: %s%n", file);

            try {
                // Determine file type based on extension using VectorFileExtension enum
                VectorFileExtension vectorFileExtension = VectorFileExtension.fromExtension(fileExtension);

                if (vectorFileExtension == null) {
                    logger.error("Unsupported file type: {}", fileExtension);
                    return 1;
                }

                FileType fileType = vectorFileExtension.getFileType();
                Class<?> dataType = vectorFileExtension.getDataType();

                describeFile(file, dataType, fileType);

                return 0;
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

    /// Calculate the size of a record in bytes based on data type, dimensions, and file type
    /// 
    /// @param dataTypeStr The data type as a string ("float", "int", etc.)
    /// @param dimensions The number of dimensions in the vector
    /// @param fileType The file type (FileType.xvec, FileType.parquet, etc.)
    /// @return The size of a record in bytes
    private int calculateRecordSizeBytes(String dataTypeStr, int dimensions, FileType fileType) {
        // Base size in bytes for different data types
        int elementSize;
        switch (dataTypeStr.toLowerCase()) {
            case "float":
                elementSize = 4;  // 4 bytes per float
                break;
            case "int":
                elementSize = 4;    // 4 bytes per int
                break;
            case "double":
                elementSize = 8; // 8 bytes per double
                break;
            default:
                elementSize = 4;       // Default to 4 bytes
                break;
        }

        // Calculate record size based on file type
        switch (fileType) {
            case xvec:
                return 4 + (dimensions * elementSize); // 4 bytes for dimension + data
            case parquet:
                return dimensions * elementSize;    // Parquet has its own metadata overhead
            case csv:
                return dimensions * elementSize;        // CSV is text-based, this is approximate
            default:
                return dimensions * elementSize;         // Default calculation
        }
    }

    /// Describe a vector file
    /// 
    /// @param file The file to process
    /// @param dataType The class representing the data type (float[].class or int[].class)
    /// @param fileType The file type (FileType.xvec, FileType.parquet, etc.)
    private <T> void describeFile(Path file, Class<T> dataType, FileType fileType) {
        try {
            // Open the file using VectorFileIO
            VectorFileArray<T> vectorArray = VectorFileIO.randomAccess(fileType, dataType, file);
            int vectorCount = vectorArray.getSize();

            // Get the dimensions by examining the first vector (if available)
            int dimensions = 0;
            String dataTypeStr = "unknown";

            if (vectorCount > 0) {
                T vector = vectorArray.get(0);

                if (vector instanceof float[]) {
                    float[] floatVector = (float[]) vector;
                    dimensions = floatVector.length;
                    dataTypeStr = "float";
                } else if (vector instanceof int[]) {
                    int[] intVector = (int[]) vector;
                    dimensions = intVector.length;
                    dataTypeStr = "int";
                } else {
                    dataTypeStr = vector.getClass().getSimpleName();
                }
            }

            // Calculate record size in bytes
            int recordSizeBytes = calculateRecordSizeBytes(dataTypeStr, dimensions, fileType);

            // Check if vectors are normalized (for dot product compatibility)
            boolean isNormalized = false;
            String normalizationStatus = "Unknown";
            if (dataTypeStr.equals("float") && vectorCount > 0) {
                try {
                    isNormalized = io.nosqlbench.command.compute.VectorNormalizationDetector.areVectorsNormalized(file);
                    normalizationStatus = isNormalized ? "NORMALIZED (||v||=1.0)" : "NOT NORMALIZED";
                } catch (Exception e) {
                    logger.debug("Could not detect vector normalization: {}", e.getMessage());
                    normalizationStatus = "Unknown (check failed)";
                }
            } else if (!dataTypeStr.equals("float")) {
                normalizationStatus = "N/A (not float vectors)";
            }

            // Print the description
            System.out.println("File Description:");
            System.out.printf("- File: %s%n", file);
            System.out.printf("- File Type: %s%n", fileType);
            System.out.printf("- Data Type: %s%n", dataTypeStr);
            System.out.printf("- Dimensions: %d%n", dimensions);
            System.out.printf("- Vector Count: %d%n", vectorCount);
            System.out.printf("- Record Size: %d bytes%n", recordSizeBytes);
            System.out.printf("- Normalization: %s%n", normalizationStatus);

            // Add helpful note about dot product compatibility
            if (dataTypeStr.equals("float")) {
                if (isNormalized) {
                    System.out.println("- Dot Product: ✓ Safe to use DOT_PRODUCT metric (vectors are normalized)");
                } else if (normalizationStatus.startsWith("NOT")) {
                    System.out.println("- Dot Product: ✗ DO NOT use DOT_PRODUCT metric (vectors not normalized)");
                    System.out.println("               Use EUCLIDEAN or COSINE instead");
                }
            }

            // Close the vector array
            vectorArray.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
