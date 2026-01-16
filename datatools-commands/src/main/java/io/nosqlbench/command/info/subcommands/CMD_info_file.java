package io.nosqlbench.command.info.subcommands;

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
import picocli.CommandLine;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

/// Show metadata and structure of a vector file.
///
/// This command displays detailed information about a vector file including
/// its format, dimensions, vector count, and file size statistics.
///
/// ## Supported Formats
///
/// - `.fvec` - Float vectors (4 bytes per component)
/// - `.ivec` - Integer vectors (4 bytes per component)
/// - `.bvec` - Byte vectors (1 byte per component)
/// - `.dvec` - Double vectors (8 bytes per component)
///
/// ## Usage
///
/// ```bash
/// nbvectors info file --input data.fvec
/// nbvectors info file -i queries.fvec --sample 5
/// ```
///
/// ## Output
///
/// Shows detailed file information including:
/// - File path and size
/// - Last modified timestamp
/// - Format and endianness
/// - Dimensions and vector count
/// - Memory requirements
/// - Optional sample vectors
@CommandLine.Command(
    name = "file",
    header = "Show vector file metadata",
    description = "Displays detailed information about a vector file including format, dimensions, and structure.",
    exitCodeList = {
        "0: Success",
        "1: Error reading file"
    }
)
public class CMD_info_file implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_info_file.class);

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_CYAN = "\u001B[96m";

    private static final DateTimeFormatter DATE_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @CommandLine.Option(
        names = {"--input", "-i"},
        description = "Path to the vector file",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"--sample", "-s"},
        description = "Number of sample vectors to display (default: 0)"
    )
    private int sampleCount = 0;

    @CommandLine.Option(
        names = {"--hex"},
        description = "Show raw hex bytes for sample vectors"
    )
    private boolean showHex = false;

    @Override
    public Integer call() {
        try {
            if (!Files.exists(inputPath)) {
                System.err.println("Error: File not found: " + inputPath);
                return 1;
            }

            String fileName = inputPath.getFileName().toString().toLowerCase();
            String extension = getExtension(fileName);

            if (!isVectorFormat(extension)) {
                System.err.println("Warning: Unrecognized vector format: " + extension);
                System.err.println("Supported formats: .fvec, .ivec, .bvec, .dvec");
            }

            // Get file metadata
            long fileSize = Files.size(inputPath);
            Instant lastModified = Files.getLastModifiedTime(inputPath).toInstant();

            // Read vector header
            VectorFileInfo info = readVectorFileInfo(extension);

            // Print report
            printFileInfo(fileSize, lastModified, extension, info);

            // Print sample vectors if requested
            if (sampleCount > 0) {
                printSampleVectors(extension, info, Math.min(sampleCount, info.vectorCount));
            }

            return 0;

        } catch (Exception e) {
            logger.error("Error reading file", e);
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private void printFileInfo(long fileSize, Instant lastModified, String extension, VectorFileInfo info) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                           VECTOR FILE INFORMATION                             ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();

        System.out.println(ANSI_CYAN + "File Details:" + ANSI_RESET);
        System.out.printf("  Path:              %s%n", inputPath.toAbsolutePath());
        System.out.printf("  Size:              %s (%,d bytes)%n", formatFileSize(fileSize), fileSize);
        System.out.printf("  Last Modified:     %s%n", DATE_FORMATTER.format(lastModified));
        System.out.println();

        System.out.println(ANSI_CYAN + "Format:" + ANSI_RESET);
        System.out.printf("  Extension:         %s%n", extension);
        System.out.printf("  Data Type:         %s%n", info.dataType);
        System.out.printf("  Bytes per Value:   %d%n", info.bytesPerValue);
        System.out.printf("  Endianness:        Little-endian (standard xvec format)%n");
        System.out.println();

        System.out.println(ANSI_CYAN + "Structure:" + ANSI_RESET);
        System.out.printf("  Dimensions:        %,d%n", info.dimensions);
        System.out.printf("  Vector Count:      %,d%n", info.vectorCount);
        System.out.printf("  Bytes per Vector:  %,d (4 header + %d data)%n",
            info.bytesPerVector, info.bytesPerVector - 4);
        System.out.println();

        System.out.println(ANSI_CYAN + "Memory Requirements:" + ANSI_RESET);
        long dataBytes = (long) info.vectorCount * info.dimensions * info.bytesPerValue;
        System.out.printf("  Raw Data:          %s%n", formatFileSize(dataBytes));
        System.out.printf("  With Headers:      %s%n", formatFileSize(fileSize));
        System.out.printf("  Header Overhead:   %.2f%%%n",
            100.0 * (fileSize - dataBytes) / fileSize);
        System.out.println();

        // Validation check
        long expectedSize = (long) info.vectorCount * info.bytesPerVector;
        if (fileSize != expectedSize) {
            System.out.println(ANSI_BOLD + "⚠ Warning:" + ANSI_RESET +
                String.format(" File size mismatch! Expected %,d bytes, got %,d bytes.", expectedSize, fileSize));
            long remainder = fileSize % info.bytesPerVector;
            if (remainder > 0) {
                System.out.printf("  Trailing bytes: %d (possible truncation or corruption)%n", remainder);
            }
            System.out.println();
        }
    }

    private void printSampleVectors(String extension, VectorFileInfo info, int count) throws IOException {
        System.out.println(ANSI_CYAN + "Sample Vectors:" + ANSI_RESET);
        System.out.println();

        try (RandomAccessFile raf = new RandomAccessFile(inputPath.toFile(), "r")) {
            ByteBuffer buffer = ByteBuffer.allocate(info.bytesPerVector).order(ByteOrder.LITTLE_ENDIAN);

            for (int v = 0; v < count; v++) {
                raf.seek((long) v * info.bytesPerVector);
                buffer.clear();
                raf.getChannel().read(buffer);
                buffer.flip();

                int headerDims = buffer.getInt();
                System.out.printf("  Vector %d (header dims: %d):%n", v, headerDims);

                if (showHex) {
                    // Show raw hex
                    System.out.print("    Hex: ");
                    for (int d = 0; d < Math.min(info.dimensions, 8); d++) {
                        if (extension.equals(".bvec")) {
                            System.out.printf("%02X ", buffer.get() & 0xFF);
                        } else if (extension.equals(".dvec")) {
                            System.out.printf("%016X ", Double.doubleToLongBits(buffer.getDouble()));
                        } else {
                            System.out.printf("%08X ", buffer.getInt());
                        }
                    }
                    if (info.dimensions > 8) {
                        System.out.print("...");
                    }
                    System.out.println();
                } else {
                    // Show values
                    System.out.print("    Values: [");
                    buffer.position(4); // Skip header
                    for (int d = 0; d < Math.min(info.dimensions, 8); d++) {
                        if (d > 0) System.out.print(", ");
                        switch (extension) {
                            case ".fvec" -> System.out.printf("%.4f", buffer.getFloat());
                            case ".ivec" -> System.out.printf("%d", buffer.getInt());
                            case ".bvec" -> System.out.printf("%d", buffer.get() & 0xFF);
                            case ".dvec" -> System.out.printf("%.6f", buffer.getDouble());
                            default -> System.out.printf("%.4f", buffer.getFloat());
                        }
                    }
                    if (info.dimensions > 8) {
                        System.out.print(", ...");
                    }
                    System.out.println("]");
                }
            }
        }
        System.out.println();
    }

    private VectorFileInfo readVectorFileInfo(String extension) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(inputPath.toFile(), "r")) {
            byte[] dimBytes = new byte[4];
            raf.read(dimBytes);
            int dimensions = ByteBuffer.wrap(dimBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

            int bytesPerValue = switch (extension) {
                case ".fvec", ".ivec" -> 4;
                case ".bvec" -> 1;
                case ".dvec" -> 8;
                default -> 4; // Assume float
            };

            String dataType = switch (extension) {
                case ".fvec" -> "float32";
                case ".ivec" -> "int32";
                case ".bvec" -> "uint8";
                case ".dvec" -> "float64";
                default -> "unknown";
            };

            int bytesPerVector = 4 + dimensions * bytesPerValue;
            int vectorCount = (int) (raf.length() / bytesPerVector);

            return new VectorFileInfo(dimensions, vectorCount, bytesPerVector, bytesPerValue, dataType);
        }
    }

    private String getExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(lastDot) : "";
    }

    private boolean isVectorFormat(String extension) {
        return extension.equals(".fvec") || extension.equals(".ivec") ||
               extension.equals(".bvec") || extension.equals(".dvec");
    }

    private String formatFileSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    private record VectorFileInfo(int dimensions, int vectorCount, int bytesPerVector, int bytesPerValue, String dataType) {}
}
