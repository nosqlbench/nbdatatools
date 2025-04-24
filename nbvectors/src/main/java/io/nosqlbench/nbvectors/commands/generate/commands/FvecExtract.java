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
        names = {"--count"},
        description = "Number of indices to use from ivec file",
        required = true
    )
    private int count;

    @CommandLine.Option(
        names = {"--force"},
        description = "Force overwrite of output file if it exists",
        defaultValue = "false"
    )
    private boolean force;

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

            // Ensure count is within bounds
            if (count <= 0 || count > ivecSize) {
                logger.error("Count must be positive and not greater than the number of indices in ivec file: {}", ivecSize);
                return 1;
            }

            // Validate all indices before writing output
            for (int i = 0; i < count; i++) {
                int[] indexVector = ivecReader.get(i);
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

            // Create output file after validation
            outputStream = new DataOutputStream(Files.newOutputStream(outputPath));

            logger.info("Extracting {} vectors from {} using indices from {}", count, fvecPath, ivecPath);
            
            // Extract and write vectors
            for (int i = 0; i < count; i++) {
                int index = ivecReader.get(i)[0];
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
