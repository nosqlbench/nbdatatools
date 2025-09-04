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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Count zero vectors in vector files
/// 
/// This command counts the number of "zero vectors" in any supported file type.
/// A zero vector is one which has zero as its value for every dimensional component.
/// The command provides progress bars for both overall progress and per-file progress.
/// At the end, a summary is printed for each file showing the number of zero vectors
/// and the total number of vectors scanned.
@CommandLine.Command(name = "count_zeros",
    header = "Count zero vectors in vector files",
    description = "Counts the number of zero vectors in any supported file type",
    exitCodeList = {"0: success", "1: error processing files"})
public class CMD_analyze_zeros implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_zeros.class);

    @CommandLine.Parameters(description = "Files to count zero vectors in", arity = "1..*")
    private List<Path> files = new ArrayList<>();

    /// Execute the command to count zero vectors in the specified files
    /// 
    /// @return 0 for success, 1 for error
    @Override
    public Integer call() {
        try {
            int totalFiles = files.size();
            int currentFileIndex = 0;

            for (Path file : files) {
                currentFileIndex++;
                if (!Files.exists(file)) {
                    logger.error("File not found: {}", file);
                    continue;
                }

                String fileExtension = getFileExtension(file);
                long zeroVectors = 0;
                long totalVectors = 0;

                System.out.printf("Processing file %d/%d: %s%n", currentFileIndex, totalFiles, file);

                try {
                    // Determine file type based on extension
                    FileType fileType;
                    Class<?> dataType;

                    switch (fileExtension.toLowerCase()) {
                        case "fvec":
                        case "fvecs":
                            fileType = FileType.xvec;
                            dataType = float[].class;
                            CountResult result = countZerosInFile(file, dataType, fileType, currentFileIndex, totalFiles);
                            zeroVectors = result.zeroVectors;
                            totalVectors = result.totalVectors;
                            break;
                        case "ivec":
                        case "ivecs":
                            fileType = FileType.xvec;
                            dataType = int[].class;
                            CountResult result2 = countZerosInFile(file, dataType, fileType, currentFileIndex, totalFiles);
                            zeroVectors = result2.zeroVectors;
                            totalVectors = result2.totalVectors;
                            break;
                        default:
                            logger.error("Unsupported file type: {}", fileExtension);
                            continue;
                    }

                    // Print summary for this file
                    System.out.printf("Summary for %s: %d zero vectors out of %d total vectors (%.2f%%)%n",
                        file, zeroVectors, totalVectors, 
                        totalVectors > 0 ? (zeroVectors * 100.0 / totalVectors) : 0);
                } catch (Exception e) {
                    logger.error("Error processing file {}: {}", file, e.getMessage());
                }
            }

            return 0;
        } catch (Exception e) {
            logger.error("Error processing files", e);
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

    /// Count zero vectors in a file using VectorFileIO
    /// 
    /// @param file The file to process
    /// @param dataType The class representing the data type (float[].class or int[].class)
    /// @param fileType The file type (usually FileType.xvec)
    /// @param currentFileIndex The index of the current file in the list of files
    /// @param totalFiles The total number of files to process
    /// @return A CountResult containing the number of zero vectors and total vectors
    private <T> CountResult countZerosInFile(Path file, Class<T> dataType, FileType fileType, 
                                            int currentFileIndex, int totalFiles) {
        long zeroVectors = 0;
        long totalVectors = 0;

        try {
            // Open the file using VectorFileIO
            VectorFileArray<T> vectorArray = VectorFileIO.randomAccess(fileType, dataType, file);
            int vectorCount = vectorArray.getSize();

            // Process each vector
            for (int i = 0; i < vectorCount; i++) {
                T vector = vectorArray.get(i);
                boolean isZeroVector = isZeroVector(vector);

                if (isZeroVector) {
                    zeroVectors++;
                }
                totalVectors++;

                // Update progress
                double fileProgress = (double) (i + 1) / vectorCount;
                double overallProgress = (currentFileIndex - 1 + fileProgress) / totalFiles;

                // Print progress bars
                System.out.print("\r");
                System.out.print("Overall progress: " + createProgressBar(overallProgress, 30));
                System.out.print(" File progress: " + createProgressBar(fileProgress, 30));
                System.out.print(" Vectors: " + totalVectors + " Zeros: " + zeroVectors);
            }
            System.out.println(); // New line after progress bar

            // Close the vector array
            vectorArray.close();
        } catch (Exception e) {
            logger.error("Error processing file {}: {}", file, e.getMessage());
            throw new RuntimeException("Failed to process file: " + e.getMessage(), e);
        }

        return new CountResult(zeroVectors, totalVectors);
    }

    /// Check if a vector is a zero vector (all components are zero)
    /// 
    /// @param vector The vector to check
    /// @return true if all components are zero, false otherwise
    @SuppressWarnings("unchecked")
    private <T> boolean isZeroVector(T vector) {
        if (vector instanceof float[]) {
            float[] floatVector = (float[]) vector;
            for (float value : floatVector) {
                if (value != 0.0f) {
                    return false;
                }
            }
            return true;
        } else if (vector instanceof int[]) {
            int[] intVector = (int[]) vector;
            for (int value : intVector) {
                if (value != 0) {
                    return false;
                }
            }
            return true;
        }

        logger.warn("Unsupported vector type: {}", vector.getClass().getName());
        return false;
    }

    /// Create a text-based progress bar
    /// 
    /// @param percent The percentage of completion (0.0 to 1.0)
    /// @param width The width of the progress bar in characters
    /// @return A string representing the progress bar
    private String createProgressBar(double percent, int width) {
        int completed = (int) (percent * width);
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < width; i++) {
            if (i < completed) {
                sb.append("=");
            } else if (i == completed) {
                sb.append(">");
            } else {
                sb.append(" ");
            }
        }
        sb.append("] ");
        sb.append(String.format("%.1f%%", percent * 100));
        return sb.toString();
    }

    /// Class to hold the result of counting zero vectors
    private static class CountResult {
        final long zeroVectors;
        final long totalVectors;

        CountResult(long zeroVectors, long totalVectors) {
            this.zeroVectors = zeroVectors;
            this.totalVectors = totalVectors;
        }
    }
}