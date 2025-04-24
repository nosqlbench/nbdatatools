package io.nosqlbench.nbvectors.commands.generate.commands;

import io.nosqlbench.readers.UniformFvecReader;
import io.nosqlbench.readers.UniformIvecReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Extract vectors from an fvec file using indices from an ivec file.
 */
@CommandLine.Command(
    name = "fvec-extract",
    description = "Extract vectors from a fvec file using indices from an ivec file"
)
public class FvecExtract implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(FvecExtract.class);

    @CommandLine.Option(
        names = {"--ivec-file"},
        description = "Path to ivec file containing indices",
        required = true
    )
    private String ivecFile;

    @CommandLine.Option(
        names = {"--range"},
        description = "Range of indices to use (format: start..end), e.g. 0..1000 or 2000..3000",
        required = true
    )
    private String range;
    
    @CommandLine.Option(
        names = {"--fvec-file"},
        description = "Path to fvec file containing vectors to extract",
        required = true
    )
    private String fvecFile;

    @CommandLine.Option(
        names = {"--output"},
        description = "Path to output fvec file",
        required = true
    )
    private String outputFile;

    @CommandLine.Option(
        names = {"--force"},
        description = "Force overwrite of output file if it exists",
        defaultValue = "false"
    )
    private boolean force;

    /// Parse a range string in the format "start..end" into a long array with \[start, end] values
    /// @param rangeStr String in format "start..end"
    /// @return long array with \[startIndex, endIndex]
    /// @throws IllegalArgumentException if the range format is invalid
    private long[] parseRange(String rangeStr) {
        if (rangeStr == null || !rangeStr.contains("..")) {
            throw new IllegalArgumentException("Range must be in format 'start..end'");
        }
        
        String[] parts = rangeStr.split("\\.\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Range must be in format 'start..end'");
        }
        
        try {
            long start = Long.parseLong(parts[0]);
            long end = Long.parseLong(parts[1]);
            
            if (start < 0 || end < 0) {
                throw new IllegalArgumentException("Range values cannot be negative");
            }
            
            return new long[] { start, end };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Range values must be valid numbers", e);
        }
    }
    
    @Override
    public Integer call() throws Exception {
        Path ivecPath = Paths.get(ivecFile);
        Path fvecPath = Paths.get(fvecFile);
        Path outputPath = Paths.get(outputFile);

        // Check if input files exist
        if (!Files.exists(ivecPath)) {
            logger.error("Ivec file does not exist: {}", ivecPath);
            return 1;
        }

        if (!Files.exists(fvecPath)) {
            logger.error("Fvec file does not exist: {}", fvecPath);
            return 1;
        }

        // Check if output file exists and handle accordingly
        if (Files.exists(outputPath)) {
            if (!force) {
                logger.error("Output file already exists: {}. Use --force to overwrite.", outputPath);
                return 1;
            }
            logger.warn("Overwriting existing file: {}", outputPath);
        }

        UniformIvecReader ivecReader = null;
        UniformFvecReader fvecReader = null;
        DataOutputStream outputStream = null;
        
        try {
            ivecReader = new UniformIvecReader(ivecPath);
            fvecReader = new UniformFvecReader(fvecPath);

            int ivecSize = ivecReader.getSize();
            int fvecSize = fvecReader.getSize();
            int dimension = fvecReader.getDimension();

            // Validate the range
            long[] rangeBounds = parseRange(range);
            long startIndex = rangeBounds[0];
            long endIndex = rangeBounds[1];
            
            // Ensure range is within bounds
            if (startIndex < 0) {
                logger.error("Start index must be non-negative: {}", startIndex);
                return 1;
            }
            
            if (endIndex >= ivecSize) {
                logger.warn("End index {} exceeds ivec size {}. Will only process up to index {}", 
                           endIndex, ivecSize, ivecSize - 1);
                endIndex = ivecSize - 1;
            }
            
            if (startIndex > endIndex) {
                logger.error("Start index {} is greater than end index {}", startIndex, endIndex);
                return 1;
            }
            
            // Validate all indices before writing output
            for (long i = startIndex; i <= endIndex; i++) {
                if (i >= ivecSize) {
                    logger.warn("Reached end of ivec file at index {}, will extract up to index {}", i, i - 1);
                    endIndex = i - 1;
                    break;
                }
                
                int[] indexVector = ivecReader.get((int)i);
                if (indexVector.length == 0) {
                    logger.error("Empty index vector at position {}", i);
                    return 1;
                }
                
                int index = indexVector[0];
                if (index < 0 || index >= fvecSize) {
                    logger.error("Index {} at position {} is out of bounds for fvec file (size: {})", index, i, fvecSize);
                    return 1;
                }
            }
            
            // Calculate number of vectors to extract
            long vectorCount = endIndex - startIndex + 1;
            
            // Create output file after validation
            outputStream = new DataOutputStream(Files.newOutputStream(outputPath));
            
            logger.info("Extracting {} vectors from {} using indices from {} (range: {}..{})", 
                       vectorCount, fvecPath, ivecPath, startIndex, endIndex);
            
            // Extract and write vectors
            for (long i = startIndex; i <= endIndex; i++) {
                int[] indexVector = ivecReader.get((int)i);
                int index = indexVector[0];
                float[] vector = fvecReader.get(index);
                outputStream.writeInt(dimension);
                for (float value : vector) {
                    outputStream.writeFloat(value);
                }
            }

            logger.info("Successfully wrote extracted vectors to {}", outputPath);
            return 0;

        } catch (IOException e) {
            logger.error("Error during extraction: {}", e.getMessage(), e);
            return 2;
        } finally {
            // Close resources in finally block
            try {
                if (ivecReader != null) {
                    ivecReader.close();
                }
                if (fvecReader != null) {
                    fvecReader.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (Exception e) {
                logger.error("Error closing resources: {}", e.getMessage(), e);
            }
        }
    }
}
