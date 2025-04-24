package io.nosqlbench.nbvectors.commands.generate.commands;

import io.nosqlbench.readers.UniformFvecReader;
import io.nosqlbench.writers.UniformFvecWriter;
import io.nosqlbench.readers.UniformIvecReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
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
    
    /// Display progress information for the extraction process
    /// @param processed Number of vectors processed so far
    /// @param total Total number of vectors to process
    /// @param startTime Time when the extraction started
    private void displayProgress(long processed, long total, Instant startTime) {
        Instant now = Instant.now();
        Duration elapsed = Duration.between(startTime, now);
        
        // Calculate processing rate (vectors per second)
        double vectorsPerSecond = processed / Math.max(0.001, elapsed.toMillis() / 1000.0);
        
        // Clear the current line
        System.out.print("\r");
        
        StringBuilder progress = new StringBuilder();
        progress.append("Extracting: ").append(processed).append("/").append(total);
        
        // Add percentage
        double percentage = (double) processed / total * 100;
        progress.append(String.format(" [%.1f%%]", percentage));
        
        // Add estimated time remaining
        if (processed > 0 && vectorsPerSecond > 0) {
            long remainingSeconds = (long) ((total - processed) / vectorsPerSecond);
            progress.append(String.format(", ETA: %02d:%02d:%02d", 
                remainingSeconds / 3600, (remainingSeconds % 3600) / 60, remainingSeconds % 60));
        }
        
        // Add rate information
        progress.append(String.format(" | %.1f vectors/sec", vectorsPerSecond));
        
        // Add elapsed time
        long elapsedSeconds = elapsed.getSeconds();
        progress.append(String.format(" | Time: %02d:%02d:%02d", 
            elapsedSeconds / 3600, (elapsedSeconds % 3600) / 60, elapsedSeconds % 60));
        
        // Display progress bar
        progress.append("\n[");
        int barWidth = 50;
        int completedWidth = (int) ((processed * barWidth) / (double) total);
        
        for (int i = 0; i < barWidth; i++) {
            if (i < completedWidth) {
                progress.append("=");
            } else if (i == completedWidth) {
                progress.append(">");
            } else {
                progress.append(" ");
            }
        }
        progress.append("]");
        
        // Output progress directly to stdout
        System.out.print(progress.toString());
        System.out.flush();
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
        UniformFvecWriter fvecWriter = null;
        
        try {
            // Open the readers
            ivecReader = new UniformIvecReader(ivecPath);
            fvecReader = new UniformFvecReader(fvecPath);
            
            // Get the sizes
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
            
            // Fail-fast check: access the minimum and maximum indices in the range to verify validity
            try {
                // Check the first index in the range
                int[] firstIndexVector = ivecReader.get((int)startIndex);
                int firstIndex = firstIndexVector[0];
                if (firstIndex < 0 || firstIndex >= fvecSize) {
                    logger.error("The first index {} in range (at position {}) is out of bounds for fvec file (size: {})", 
                                firstIndex, startIndex, fvecSize);
                    return 1;
                }
                
                // Check the last index in the range
                int[] lastIndexVector = ivecReader.get((int)endIndex);
                int lastIndex = lastIndexVector[0];
                if (lastIndex < 0 || lastIndex >= fvecSize) {
                    logger.error("The last index {} in range (at position {}) is out of bounds for fvec file (size: {})", 
                                lastIndex, endIndex, fvecSize);
                    return 1;
                }
                
                System.out.println("Range validation successful: first index=" + firstIndex + ", last index=" + lastIndex);
            } catch (Exception e) {
                logger.error("Failed to validate index range: {}", e.getMessage());
                return 1;
            }
            
            // Calculate number of vectors to extract
            long vectorCount = endIndex - startIndex + 1;
            
            // Create output file with writer
            fvecWriter = new UniformFvecWriter(outputPath, dimension);
            
            logger.info("Extracting {} vectors from {} using indices from {} (range: {}..{})", 
                       vectorCount, fvecPath, ivecPath, startIndex, endIndex);
            
            // Setup progress tracking
            Instant startTime = Instant.now();
            long processedVectors = 0;
            long nextReportTime = System.currentTimeMillis() + 1000; // First report after 1 second
            
            // Initial status message to stdout
            System.out.println("Starting vector extraction...");
            
            // Extract and write vectors directly as we process them
            for (long i = startIndex; i <= endIndex; i++) {
                try {
                    // Get the index from ivec file
                    int[] indexVector = ivecReader.get((int)i);
                    int index = indexVector[0];
                    
                    // Get the vector from fvec file
                    float[] vector = fvecReader.get(index);
                    
                    // Write to output file using the writer
                    fvecWriter.write(vector);
                    
                    // Update progress
                    processedVectors++;
                    
                    // Display progress at most once per second
                    long currentTime = System.currentTimeMillis();
                    if (currentTime >= nextReportTime) {
                        displayProgress(processedVectors, vectorCount, startTime);
                        nextReportTime = currentTime + 1000;
                    }
                } catch (Exception e) {
                    logger.error("Error processing vector at index {}: {}", i, e.getMessage());
                    return 1;
                }
            }
            
            // Display final progress
            displayProgress(processedVectors, vectorCount, startTime);
            System.out.println(); // Add newline after progress bar
            System.out.println("Vector extraction completed successfully.");

            logger.info("Successfully wrote extracted vectors to {}", outputPath);
            return 0;

        } catch (Exception e) {
            // Handle invalid arguments or parsing errors
            if (e instanceof IllegalArgumentException) {
                logger.error("{}", e.getMessage());
                return 1;
            }
            // Other errors (I/O or unexpected)
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
                if (fvecWriter != null) {
                    fvecWriter.close();
                }
            } catch (Exception e) {
                logger.error("Error closing resources: {}", e.getMessage(), e);
            }
        }
    }
}
