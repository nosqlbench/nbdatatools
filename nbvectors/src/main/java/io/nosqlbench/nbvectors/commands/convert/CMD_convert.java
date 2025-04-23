package io.nosqlbench.nbvectors.commands.convert;

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

import io.nosqlbench.nbvectors.services.Selector;
import io.nosqlbench.readers.SizedReader;
import io.nosqlbench.readers.SizedReaderLookup;
import io.nosqlbench.readers.SizedStreamerLookup;
import io.nosqlbench.streamers.SizedStreamer;
import io.nosqlbench.streamers.Streamer;
import io.nosqlbench.streamers.StreamerLookup;
import io.nosqlbench.writers.Writer;
import io.nosqlbench.writers.WriterLookup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * # Vector Format Conversion Tool
 * 
 * This command converts vector data between different file formats:
 * - fvec: float vector format (4-byte float values)
 * - ivec: integer vector format (4-byte integer values)
 * - bvec: binary vector format (1-byte unsigned values)
 * - csv: comma-separated text format
 * - json: JSON-based vector format
 * 
 * # Basic Usage
 * ```
 * convert --input vectors.fvec --output vectors.csv
 * ```
 * 
 * # Data Flow
 * ```
 * ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
 * │ Input         │────▶│ Optional      │────▶│ Output        │
 * │ Vector File   │     │ Transformations│     │ Vector File   │
 * └───────────────┘     └───────────────┘     └───────────────┘
 * ```
 * 
 * Formats are automatically detected by file extension.
 */
@Selector("convert")
@CommandLine.Command(name = "convert",
    header = "Convert between different vector file formats",
    description = """
        Converts vectors between different file formats including fvec, ivec, bvec, csv, and json.
        Formats are automatically detected from file extensions.""",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_convert implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_convert.class);
    
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_WARNING = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(
        names = {"-i", "--input"},
        description = "Input vector file path (supports fvec, ivec, bvec, csv, json formats)",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"-o", "--output"},
        description = "Output vector file path",
        required = true
    )
    private Path outputPath;

    @CommandLine.Option(
        names = {"--input-format"},
        description = "Explicitly specify input format (overrides file extension detection)"
    )
    private String inputFormat;

    @CommandLine.Option(
        names = {"--output-format"},
        description = "Explicitly specify output format (overrides file extension detection)"
    )
    private String outputFormat;

    @CommandLine.Option(
        names = {"-f", "--force"},
        description = "Force overwrite if output file already exists"
    )
    private boolean force = false;

    @CommandLine.Option(
        names = {"--normalize"},
        description = "Normalize vector magnitudes to 1.0 (L2 normalization)"
    )
    private boolean normalize = false;

    @CommandLine.Option(
        names = {"--precision"},
        description = "Set output precision for floating-point values (CSV/JSON formats)",
        defaultValue = "6"
    )
    private int precision = 6;

    @CommandLine.Option(
        names = {"--limit"},
        description = "Limit the number of vectors to convert"
    )
    private Integer limit;

    @CommandLine.Option(
        names = {"--offset"},
        description = "Start converting from this vector index (0-based)",
        defaultValue = "0"
    )
    private int offset = 0;

    @CommandLine.Option(
        names = {"-v", "--verbose"},
        description = "Enable verbose output"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "Display this help message"
    )
    private boolean helpRequested = false;

    public static void main(String[] args) {
        CMD_convert cmd = new CMD_convert();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            // Check if output file exists
            if (Files.exists(outputPath) && !force) {
                logger.error("Output file already exists: {}. Use --force to overwrite.", outputPath);
                return EXIT_WARNING;
            }
            
            // Create parent directories if needed
            Path parent = outputPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Determine formats based on file extensions or explicit options
            String detectedInputFormat = inputFormat != null ? inputFormat : getFormatFromPath(inputPath);
            String detectedOutputFormat = outputFormat != null ? outputFormat : getFormatFromPath(outputPath);
            
            logger.info("Converting from {} to {}", detectedInputFormat, detectedOutputFormat);
            
            // For float vector input
            if (isFloatFormat(detectedInputFormat)) {
                return convertFloatVectors(detectedInputFormat, detectedOutputFormat);
            } 
            // For integer vector input
            else if (isIntFormat(detectedInputFormat)) {
                return convertIntVectors(detectedInputFormat, detectedOutputFormat);
            } 
            // For double vector input
            else if (isDoubleFormat(detectedInputFormat)) {
                logger.error("Double vector conversion not yet implemented");
                return EXIT_ERROR;
            }
            // Unsupported format combination
            else {
                logger.error("Unsupported conversion from {} to {}", detectedInputFormat, detectedOutputFormat);
                return EXIT_ERROR;
            }
            
        } catch (Exception e) {
            logger.error("Error during conversion: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }
    
    /**
     * Determine the data type class based on file extension
     * 
     * @param fileExtension The file extension (e.g., "fvec", "ivec", "bvec", "dvec")
     * @return The appropriate data type class
     */
    private Class<?> inferDataTypeFromExtension(String fileExtension) {
        switch (fileExtension.toLowerCase()) {
            case "fvec":
                return float[].class;
            case "ivec":
            case "bvec":
                return int[].class;
            case "dvec":
                return double[].class;
            default:
                // Default to float[] for unknown extensions
                return float[].class;
        }
    }
    
    /**
     * Check if the format is a double vector format
     */
    private boolean isDoubleFormat(String format) {
        return format.equalsIgnoreCase("dvec");
    }
    
    /**
     * Convert float vector data from input to output format
     */
    private Integer convertFloatVectors(String inputFormat, String outputFormat) throws Exception {
        // Find appropriate writer
        Optional<Writer<float[]>> writerOpt = WriterLookup.findWriter(outputFormat, float[].class);
        if (writerOpt.isEmpty()) {
            logger.error("No suitable writer found for format: {}", outputFormat);
            return EXIT_ERROR;
        }
        
        Writer<float[]> writer = writerOpt.get();
        writer.initialize(outputPath);
        
        try {
            // Try to find a reader in order of preference: SizedStreamer, Streamer, SizedReader
            Iterator<float[]> iterator = null;
            String sourceName = "unknown";
            long totalVectors = -1;
            
            // Try SizedStreamer
            Optional<SizedStreamer<float[]>> sizedStreamer = SizedStreamerLookup.findReader(inputFormat, float[].class);
            if (sizedStreamer.isPresent()) {
                SizedStreamer<float[]> streamer = sizedStreamer.get();
                iterator = streamer.iterator();
                totalVectors = streamer.getSize();
                sourceName = streamer.getName();
                if (verbose) {
                    logger.info("Using SizedStreamer for format: {}", inputFormat);
                }
            } 
            // Try Streamer
            else {
                Optional<Streamer<float[]>> streamer = StreamerLookup.findStreamer(inputFormat, float[].class);
                if (streamer.isPresent()) {
                    Streamer<float[]> s = streamer.get();
                    iterator = s.iterator();
                    sourceName = s.getName();
                    if (verbose) {
                        logger.info("Using Streamer for format: {}", inputFormat);
                    }
                } 
                // Try SizedReader
                else {
                    Optional<SizedReader<float[]>> reader = SizedReaderLookup.findReader(inputFormat, float[].class);
                    if (reader.isPresent()) {
                        SizedReader<float[]> r = reader.get();
                        iterator = r.iterator();
                        totalVectors = r.size();
                        sourceName = r.getName();
                        if (verbose) {
                            logger.info("Using SizedReader for format: {}", inputFormat);
                        }
                    } else {
                        logger.error("No compatible reader found for format: {}", inputFormat);
                        return EXIT_ERROR;
                    }
                }
            }
            
            // Skip initial vectors if offset > 0
            for (int i = 0; i < offset && iterator.hasNext(); i++) {
                iterator.next();
            }
            
            int count = 0;
            long adjustedTotal = totalVectors >= 0 ? totalVectors - offset : -1;
            long vectorsToProcess = limit != null ? Math.min(limit, adjustedTotal >= 0 ? adjustedTotal : Integer.MAX_VALUE) : adjustedTotal;
            
            if (verbose) {
                if (adjustedTotal >= 0) {
                    logger.info("Processing {} vectors from {} (total: {})", 
                        vectorsToProcess >= 0 ? vectorsToProcess : "all remaining", 
                        sourceName, 
                        adjustedTotal);
                } else {
                    logger.info("Processing vectors from {} (unknown total count)", sourceName);
                }
            }
            
            while (iterator.hasNext() && (limit == null || count < limit)) {
                float[] vector = iterator.next();
                
                if (normalize) {
                    normalizeVector(vector);
                }
                
                writer.write(vector);
                count++;
                
                if (verbose && count % 10000 == 0) {
                    logger.info("Processed {} vectors", count);
                }
            }
            
            if (verbose) {
                logger.info("Conversion complete. Processed {} vectors", count);
            }
            
            return EXIT_SUCCESS;
        } finally {
            writer.close();
        }
    }
    
    /**
     * Convert integer vector data from input to output format
     */
    private Integer convertIntVectors(String inputFormat, String outputFormat) throws Exception {
        // Find appropriate writer
        Optional<Writer<int[]>> writerOpt = WriterLookup.findWriter(outputFormat, int[].class);
        if (writerOpt.isEmpty()) {
            logger.error("No suitable writer found for format: {}", outputFormat);
            return EXIT_ERROR;
        }
        
        Writer<int[]> writer = writerOpt.get();
        writer.initialize(outputPath);
        
        try {
            // Try to find a reader in order of preference: SizedStreamer, Streamer, SizedReader
            Iterator<int[]> iterator = null;
            String sourceName = "unknown";
            long totalVectors = -1;
            
            // Try SizedStreamer
            Optional<SizedStreamer<int[]>> sizedStreamer = SizedStreamerLookup.findReader(inputFormat, int[].class);
            if (sizedStreamer.isPresent()) {
                SizedStreamer<int[]> streamer = sizedStreamer.get();
                iterator = streamer.iterator();
                totalVectors = streamer.getSize();
                sourceName = streamer.getName();
                if (verbose) {
                    logger.info("Using SizedStreamer for format: {}", inputFormat);
                }
            } 
            // Try Streamer
            else {
                Optional<Streamer<int[]>> streamer = StreamerLookup.findStreamer(inputFormat, int[].class);
                if (streamer.isPresent()) {
                    Streamer<int[]> s = streamer.get();
                    iterator = s.iterator();
                    sourceName = s.getName();
                    if (verbose) {
                        logger.info("Using Streamer for format: {}", inputFormat);
                    }
                } 
                // Try SizedReader
                else {
                    Optional<SizedReader<int[]>> reader = SizedReaderLookup.findReader(inputFormat, int[].class);
                    if (reader.isPresent()) {
                        SizedReader<int[]> r = reader.get();
                        iterator = r.iterator();
                        totalVectors = r.size();
                        sourceName = r.getName();
                        if (verbose) {
                            logger.info("Using SizedReader for format: {}", inputFormat);
                        }
                    } else {
                        logger.error("No compatible reader found for format: {}", inputFormat);
                        return EXIT_ERROR;
                    }
                }
            }
            
            // Skip initial vectors if offset > 0
            for (int i = 0; i < offset && iterator.hasNext(); i++) {
                iterator.next();
            }
            
            int count = 0;
            long adjustedTotal = totalVectors >= 0 ? totalVectors - offset : -1;
            long vectorsToProcess = limit != null ? Math.min(limit, adjustedTotal >= 0 ? adjustedTotal : Integer.MAX_VALUE) : adjustedTotal;
            
            if (verbose) {
                if (adjustedTotal >= 0) {
                    logger.info("Processing {} vectors from {} (total: {})", 
                        vectorsToProcess >= 0 ? vectorsToProcess : "all remaining", 
                        sourceName, 
                        adjustedTotal);
                } else {
                    logger.info("Processing vectors from {} (unknown total count)", sourceName);
                }
            }
            
            while (iterator.hasNext() && (limit == null || count < limit)) {
                int[] vector = iterator.next();
                writer.write(vector);
                count++;
                
                if (verbose && count % 10000 == 0) {
                    logger.info("Processed {} vectors", count);
                }
            }
            
            if (verbose) {
                logger.info("Conversion complete. Processed {} vectors", count);
            }
            
            return EXIT_SUCCESS;
        } finally {
            writer.close();
        }
    }
    
    /**
     * Normalize a vector to unit length (L2 normalization)
     */
    private void normalizeVector(float[] vector) {
        // Calculate vector magnitude (L2 norm)
        double sumSquares = 0.0;
        for (float v : vector) {
            sumSquares += v * v;
        }
        double magnitude = Math.sqrt(sumSquares);
        
        // Skip normalization if magnitude is zero or very close to zero
        if (magnitude < 1e-10) {
            return;
        }
        
        // Normalize each component
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) (vector[i] / magnitude);
        }
    }
    
    /**
     * Extract format name from file path extension
     * 
     * @param path The file path to extract the format from
     * @return The format string derived from the file extension
     */
    private String getFormatFromPath(Path path) {
        String filename = path.getFileName().toString().toLowerCase();
        int dotIndex = filename.lastIndexOf('.');
        
        if (dotIndex > 0 && dotIndex < filename.length() - 1) {
            return filename.substring(dotIndex + 1);
        }
        
        // Default to fvec if no extension
        return "fvec";
    }
    
    /**
     * Check if the format is a float vector format
     */
    private boolean isFloatFormat(String format) {
        return format.equals("fvec") || format.equals("csv") || format.equals("json");
    }
    
    /**
     * Check if the format is an integer vector format
     */
    private boolean isIntFormat(String format) {
        return format.equals("ivec") || format.equals("bvec");
    }
}
