package io.nosqlbench.nbvectors.commands.mktestdata;

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


import io.nosqlbench.nbvectors.util.RandomGenerators;
import io.nosqlbench.readers.SizedReader;
import io.nosqlbench.readers.SizedReaderLookup;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// # Test Data Generator Command
///
/// This command generates test data for vector search benchmarking by:
/// 1. Reading vectors from an input file
/// 2. Creating a deterministic shuffling of the vector indices
/// 3. Splitting the vectors into query and database sets
/// 4. Storing the results in output files
///
/// # Basic Usage
/// ```
/// mktestdata --input data.fvec --output-prefix test-data --queries 1000 --seed 42
/// ```
///
/// # Data Flow
/// ```
/// ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
/// │ Input Vectors │────▶│ Random        │────▶│ Index         │
/// │               │     │ Shuffling     │     │ Assignment    │
/// └───────────────┘     └───────────────┘     └───────────────┘
///                                                     │
///                                                     ▼
///                               ┌─────────────────────┬─────────────────────┐
///                               │                     │                     │
///                               ▼                     ▼                     ▼
///                       ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
///                       │ Query Vectors │    │ Database      │    │ Mapping       │
///                       │ (.fvec)       │    │ Vectors       │    │ (.ivec)       │
///                       └───────────────┘    └───────────────┘    └───────────────┘
/// ```
@CommandLine.Command(name = "mktestdata",
    header = "Make basic base, query, and ground truth data (if possible) from a vector space",
    description = """
        When given a source of vectors, create a set of conditioned base and query
        vectors in fvec format""",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_mktestdata implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_mktestdata.class);
    
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(
        names = {"-i", "--input"},
        description = "Input vector file path (supports fvec, ivec formats)",
        required = true
    )
    private Path inputPath;

    @CommandLine.Option(
        names = {"-o", "--output-prefix"},
        description = "Prefix for output files (will generate prefix_queries.fvec, prefix_db.fvec, prefix_mapping.ivec)",
        required = true
    )
    private String outputPrefix;

    @CommandLine.Option(
        names = {"-q", "--queries"},
        description = "Number of vectors to use as queries",
        required = true
    )
    private int queryCount;

    @CommandLine.Option(
        names = {"-s", "--seed"},
        description = "Random seed for reproducible shuffling",
        defaultValue = "42"
    )
    private long seed;

    @CommandLine.Option(
        names = {"-f", "--force"},
        description = "Force overwrite if output files already exist"
    )
    private boolean force = false;
    
    @CommandLine.Option(
        names = {"-a", "--algorithm"},
        description = "PRNG algorithm to use (XO_SHI_RO_256_PP, XO_SHI_RO_128_PP, SPLIT_MIX_64, MT, KISS)",
        defaultValue = "XO_SHI_RO_256_PP"
    )
    private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

    public static void main(String[] args) {
        CMD_mktestdata cmd = new CMD_mktestdata();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            // Check output paths
            Path queriesPath = Path.of(outputPrefix + "_queries.fvec");
            Path dbPath = Path.of(outputPrefix + "_db.fvec");
            Path mappingPath = Path.of(outputPrefix + "_mapping.ivec");
            
            if (!checkOutputPaths(queriesPath, dbPath, mappingPath)) {
                return EXIT_FILE_EXISTS;
            }
            
            // Create directories for output files
            createParentDirectories(queriesPath, dbPath, mappingPath);
            
            // Open input file and determine vector count
            logger.info("Reading input file: {}", inputPath);
            SizedReader<float[]> reader = openInputFile(inputPath);
            
            try {
                int vectorCount = reader.getSize();
                
                if (vectorCount == 0) {
                    logger.error("Input file contains no vectors");
                    return EXIT_ERROR;
                }
                
                if (queryCount > vectorCount) {
                    logger.error("Requested query count ({}) exceeds available vectors ({})", 
                            queryCount, vectorCount);
                    return EXIT_ERROR;
                }
                
                logger.info("Input file contains {} vectors", vectorCount);
                
                // Create shuffled indices
                List<Integer> shuffledIndices = createShuffledIndices(vectorCount);
                
                // Write mapping file
                writeMappingFile(mappingPath, shuffledIndices);
                
                // Process and write output files
                processVectors(reader, shuffledIndices, queriesPath, dbPath);
                
                logger.info("Successfully generated test data:");
                logger.info("  Queries: {} ({} vectors)", queriesPath, queryCount);
                logger.info("  Database: {} ({} vectors)", dbPath, vectorCount - queryCount);
                logger.info("  Mapping: {}", mappingPath);
                
                return EXIT_SUCCESS;
            } finally {
                // Close the reader if it's AutoCloseable
                if (reader instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) reader).close();
                    } catch (Exception e) {
                        logger.warn("Error closing reader: {}", e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error generating test data: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }
    
    /// Checks if output paths can be written to, respecting the force flag.
    private boolean checkOutputPaths(Path... paths) {
        for (Path path : paths) {
            if (Files.exists(path) && !force) {
                logger.error("Output file already exists: {}. Use --force to overwrite.", path);
                return false;
            }
        }
        return true;
    }
    
    /// Creates parent directories for all output paths.
    private void createParentDirectories(Path... paths) throws IOException {
        for (Path path : paths) {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        }
    }
    
    /// Opens the input file and returns an appropriate reader using SizedReaderLookup.
    ///
    /// @param inputPath The path to the input vector file
    /// @return A SizedReader for float vectors
    /// @throws IOException If the file doesn't exist or a reader can't be found
    private SizedReader<float[]> openInputFile(Path inputPath) throws IOException {
        if (!Files.exists(inputPath)) {
            throw new IOException("Input file does not exist: " + inputPath);
        }
        
        // Determine file format from extension
        String selector = getFormatSelector(inputPath);
        logger.info("Detected file format: {}", selector);
        
            // Use SizedReaderLookup to find and instantiate an appropriate reader with the path
            Optional<SizedReader<float[]>> reader = SizedReaderLookup.findFloatReader(selector, inputPath);
            
            if (reader.isEmpty()) {
                throw new IOException("No compatible reader found for format: " + selector + 
                        ". Ensure the appropriate reader is on the classpath.");
            }
            
            SizedReader<float[]> sizedReader = reader.get();
            logger.info("Using reader: {} (size: {})", sizedReader.getName(), sizedReader.getSize());
            
            return sizedReader;
    }
    
    /// Determines the format selector based on file extension.
    ///
    /// @param path The file path
    /// @return The format selector string (e.g., "fvec", "ivec")
    private String getFormatSelector(Path path) {
        String filename = path.getFileName().toString().toLowerCase();
        int dotIndex = filename.lastIndexOf('.');
        
        if (dotIndex > 0 && dotIndex < filename.length() - 1) {
            return filename.substring(dotIndex + 1);
        }
        
        // Default to fvec if no extension
        return "fvec";
    }
    
    /// Creates a deterministically shuffled list of indices from 0 to vectorCount-1.
    private List<Integer> createShuffledIndices(int vectorCount) {
        logger.info("Creating shuffled order with seed {}", seed);
        
        // Create sequence of integers from 0 to vectorCount-1
        List<Integer> indices = new ArrayList<>(vectorCount);
        for (int i = 0; i < vectorCount; i++) {
            indices.add(i);
        }
        
        // Create RNG with specified algorithm and seed
        RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, seed);
        
        // Shuffle the indices
        RandomGenerators.shuffle(indices, rng);
        
        return indices;
    }
    
    /// Writes the shuffled indices to a mapping file in ivec format.
    private void writeMappingFile(Path mappingPath, List<Integer> shuffledIndices) throws IOException {
        logger.info("Writing mapping file: {}", mappingPath);
        
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(mappingPath))) {
            for (Integer index : shuffledIndices) {
                dos.writeInt(1); // Dimension is 1 for each entry (scalar)
                dos.writeInt(index); // Write the index
            }
        }
    }
    
    /// Processes vectors from the input file and writes them to query and database output files.
    ///
    /// @param reader The reader for accessing input vectors
    /// @param shuffledIndices List of shuffled indices
    /// @param queriesPath Output path for query vectors
    /// @param dbPath Output path for database vectors
    /// @throws IOException If an I/O error occurs
    private void processVectors(
            SizedReader<float[]> reader, List<Integer> shuffledIndices,
                                  Path queriesPath, Path dbPath) throws IOException {
            logger.info("Processing vectors for query and database files");
            
            // Open output files
            try (DataOutputStream queriesOut = new DataOutputStream(Files.newOutputStream(queriesPath));
                 DataOutputStream dbOut = new DataOutputStream(Files.newOutputStream(dbPath))) {
                
                // Process vectors in shuffled order
                for (int i = 0; i < shuffledIndices.size(); i++) {
                    int originalIndex = shuffledIndices.get(i);
                    
                    // Use direct indexed access from SizedReader
                    float[] vector;
                    try {
                        vector = reader.get(originalIndex);
                    } catch (Exception e) {
                        throw new IOException("Failed to read vector at index " + originalIndex, e);
                }
                
                if (vector == null) {
                    throw new IOException("Failed to read vector at index " + originalIndex);
                }
                
                // Write to appropriate output file
                DataOutputStream out = (i < queryCount) ? queriesOut : dbOut;
                
                // Write dimension
                out.writeInt(vector.length);
                
                // Write vector values
                for (float value : vector) {
                    out.writeFloat(value);
                }
            }
        }
    }
}
