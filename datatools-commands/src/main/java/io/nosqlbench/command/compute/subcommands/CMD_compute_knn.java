package io.nosqlbench.command.compute.subcommands;

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

import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.nbdatatools.api.fileio.BoundedVectorFileStream;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

/// Command to compute k-nearest neighbors ground truth dataset from base and query vectors
@CommandLine.Command(name = "knn",
    description = "Compute k-nearest neighbors ground truth dataset from base and query vectors")
public class CMD_compute_knn implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_compute_knn.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(names = {"-b", "--base"}, description = "The base vectors file", required = true)
    private Path baseVectorsPath;

    @CommandLine.Option(names = {"-q", "--query"}, description = "The query vectors file", required = true)
    private Path queryVectorsPath;

    @CommandLine.Option(names = {"-o", "--output"}, description = "The output ground truth file", required = true)
    private Path outputPath;

    @CommandLine.Option(names = {"-k", "--neighbors"}, description = "Number of nearest neighbors to find", required = true)
    private int k;

    @CommandLine.Option(names = {"-d", "--distance"}, 
        description = "Distance metric to use (L2, L1, COSINE)", 
        defaultValue = "L2")
    private DistanceMetric distanceMetric = DistanceMetric.L2;

    @CommandLine.Option(names = {"-f", "--force"}, description = "Force overwrite if output file already exists")
    private boolean force = false;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    /**
     * Enum for supported distance metrics
     */
    public enum DistanceMetric {
        L2,     // Euclidean distance
        L1,     // Manhattan distance
        COSINE  // Cosine similarity
    }

    /**
     * Validates the input and output paths before execution.
     */
    private void validatePaths() {
        if (baseVectorsPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No base vectors path provided");
        }

        if (queryVectorsPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No query vectors path provided");
        }

        if (outputPath == null) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: No output path provided");
        }

        // Check if input files exist
        if (!Files.exists(baseVectorsPath)) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Base vectors file does not exist: " + baseVectorsPath);
        }

        if (!Files.exists(queryVectorsPath)) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                "Error: Query vectors file does not exist: " + queryVectorsPath);
        }

        // Normalize the paths to resolve any "." or ".." components
        baseVectorsPath = baseVectorsPath.normalize();
        queryVectorsPath = queryVectorsPath.normalize();
        outputPath = outputPath.normalize();
    }

    /**
     * Calculate distance between two vectors based on the selected distance metric
     * 
     * @param vec1 First vector
     * @param vec2 Second vector
     * @return Distance between the vectors
     */
    private double calculateDistance(float[] vec1, float[] vec2) {
        if (vec1.length != vec2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        switch (distanceMetric) {
            case L2:
                return calculateL2Distance(vec1, vec2);
            case L1:
                return calculateL1Distance(vec1, vec2);
            case COSINE:
                return calculateCosineDistance(vec1, vec2);
            default:
                throw new IllegalArgumentException("Unsupported distance metric: " + distanceMetric);
        }
    }

    /**
     * Calculate Euclidean (L2) distance between two vectors
     */
    private double calculateL2Distance(float[] vec1, float[] vec2) {
        double sum = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            double diff = vec1[i] - vec2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Calculate Manhattan (L1) distance between two vectors
     */
    private double calculateL1Distance(float[] vec1, float[] vec2) {
        double sum = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            sum += Math.abs(vec1[i] - vec2[i]);
        }
        return sum;
    }

    /**
     * Calculate Cosine distance between two vectors
     */
    private double calculateCosineDistance(float[] vec1, float[] vec2) {
        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
            norm1 += vec1[i] * vec1[i];
            norm2 += vec2[i] * vec2[i];
        }

        // Prevent division by zero
        if (norm1 == 0.0 || norm2 == 0.0) {
            return 1.0; // Maximum distance
        }

        // Cosine similarity is dot product divided by the product of the norms
        double cosineSimilarity = dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));

        // Convert to distance (1 - similarity)
        return 1.0 - cosineSimilarity;
    }

    private int determineBatchSize(int baseCount, int dimension) {
        Runtime runtime = Runtime.getRuntime();
        long maxHeapBytes = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long allocated = totalMemory - freeMemory;
        long headroom = Math.max(0L, maxHeapBytes - allocated);
        long usable = Math.max((long) (maxHeapBytes * 0.2), (long) (headroom * 0.8));

        long bytesPerVector = estimateVectorBytes(dimension);
        if (bytesPerVector <= 0) {
            bytesPerVector = Math.max(16L, (long) dimension * Float.BYTES);
        }

        long estimated = Math.max(1L, usable / bytesPerVector);
        long capped = Math.min(estimated, baseCount);
        if (capped > Integer.MAX_VALUE) {
            capped = Integer.MAX_VALUE;
        }
        return (int) Math.max(1L, capped);
    }

    private long estimateVectorBytes(int dimension) {
        final int arrayHeaderBytes = 16;
        final double safetyFactor = 2.0d;
        return (long) ((dimension * (long) Float.BYTES + arrayHeaderBytes) * safetyFactor);
    }

    private NeighborIndex[] selectTopNeighbors(float[] queryVector, List<float[]> baseBatch, int globalStartIndex, int topK) {
        if (topK <= 0 || baseBatch.isEmpty()) {
            return new NeighborIndex[0];
        }

        PriorityQueue<NeighborIndex> heap = new PriorityQueue<>(topK, Comparator.comparingDouble(NeighborIndex::distance).reversed());
        for (int i = 0; i < baseBatch.size(); i++) {
            float[] baseVector = baseBatch.get(i);
            double distance = calculateDistance(queryVector, baseVector);
            NeighborIndex candidate = new NeighborIndex(globalStartIndex + i, distance);

            if (heap.size() < topK) {
                heap.offer(candidate);
            } else if (distance < heap.peek().distance()) {
                heap.poll();
                heap.offer(candidate);
            }
        }

        int resultSize = heap.size();
        NeighborIndex[] result = new NeighborIndex[resultSize];
        for (int i = resultSize - 1; i >= 0; i--) {
            result[i] = heap.poll();
        }
        return result;
    }

    private int[] toIndexArray(NeighborIndex[] neighbors) {
        int[] indices = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++) {
            indices[i] = (int) neighbors[i].index();
        }
        return indices;
    }

    private float[] toDistanceArray(NeighborIndex[] neighbors) {
        float[] distances = new float[neighbors.length];
        for (int i = 0; i < neighbors.length; i++) {
            distances[i] = (float) neighbors[i].distance();
        }
        return distances;
    }

    private PartitionMetadata computePartition(
        VectorFileArray<float[]> baseReader,
        int partitionIndex,
        int startIndex,
        int endIndex,
        int baseDimension,
        Path neighborsPath,
        Path distancesPath,
        int effectiveK
    ) throws IOException {

        int partitionSize = endIndex - startIndex;
        if (partitionSize <= 0) {
            throw new IllegalArgumentException("Partition size must be positive");
        }

        int partitionK = Math.min(effectiveK, partitionSize);
        List<float[]> baseBatch = new ArrayList<>(partitionSize);
        for (int i = startIndex; i < endIndex; i++) {
            baseBatch.add(baseReader.get(i));
        }

        logger.info("Partition {}: loaded {} base vectors (indices [{}..{}))", partitionIndex, partitionSize, startIndex, endIndex);

        if (neighborsPath.getParent() != null) {
            Files.createDirectories(neighborsPath.getParent());
        }
        if (distancesPath.getParent() != null) {
            Files.createDirectories(distancesPath.getParent());
        }

        try (VectorFileStreamStore<int[]> neighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, neighborsPath)
                 .orElseThrow(() -> new IOException("Could not create intermediate neighbors file: " + neighborsPath));
             VectorFileStreamStore<float[]> distancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, distancesPath)
                 .orElseThrow(() -> new IOException("Could not create intermediate distances file: " + distancesPath));
             BoundedVectorFileStream<float[]> queryStream = VectorFileIO.streamIn(FileType.xvec, float[].class, queryVectorsPath)
                 .orElseThrow(() -> new IOException("Could not open query vectors file: " + queryVectorsPath))) {

            int processedQueries = 0;
            for (float[] queryVector : queryStream) {
                if (queryVector.length != baseDimension) {
                    throw new IllegalArgumentException("Query vector dimension (" + queryVector.length
                        + ") does not match base vector dimension (" + baseDimension + ")");
                }

                NeighborIndex[] neighbors = selectTopNeighbors(queryVector, baseBatch, startIndex, partitionK);
                neighborsStore.write(toIndexArray(neighbors));
                distancesStore.write(toDistanceArray(neighbors));

                processedQueries++;
                if (processedQueries % 100 == 0) {
                    System.out.println("Partition " + partitionIndex + ": processed " + processedQueries + " query vectors");
                }
            }

            logger.info("Partition {} complete: wrote {} query results (k={})", partitionIndex, processedQueries, partitionK);
            return new PartitionMetadata(neighborsPath, distancesPath, startIndex, endIndex, processedQueries, partitionK);
        }
    }

    private void mergePartitions(
        List<PartitionMetadata> partitions,
        Path finalNeighborsPath,
        Path finalDistancesPath,
        int effectiveK,
        int queryCount
    ) throws IOException {

        if (finalNeighborsPath.getParent() != null) {
            Files.createDirectories(finalNeighborsPath.getParent());
        }
        if (finalDistancesPath.getParent() != null) {
            Files.createDirectories(finalDistancesPath.getParent());
        }

        List<BoundedVectorFileStream<int[]>> neighborStreams = new ArrayList<>();
        List<BoundedVectorFileStream<float[]>> distanceStreams = new ArrayList<>();

        try {
            for (PartitionMetadata partition : partitions) {
                neighborStreams.add(VectorFileIO.streamIn(FileType.xvec, int[].class, partition.neighborsPath)
                    .orElseThrow(() -> new IOException("Could not open intermediate neighbors file: " + partition.neighborsPath)));
                distanceStreams.add(VectorFileIO.streamIn(FileType.xvec, float[].class, partition.distancesPath)
                    .orElseThrow(() -> new IOException("Could not open intermediate distances file: " + partition.distancesPath)));
            }

            List<Iterator<int[]>> neighborIterators = new ArrayList<>(neighborStreams.size());
            List<Iterator<float[]>> distanceIterators = new ArrayList<>(distanceStreams.size());

            for (BoundedVectorFileStream<int[]> stream : neighborStreams) {
                neighborIterators.add(stream.iterator());
            }
            for (BoundedVectorFileStream<float[]> stream : distanceStreams) {
                distanceIterators.add(stream.iterator());
            }

            try (VectorFileStreamStore<int[]> finalNeighborsStore = VectorFileIO.streamOut(FileType.xvec, int[].class, finalNeighborsPath)
                     .orElseThrow(() -> new IOException("Could not create final neighbors file: " + finalNeighborsPath));
                 VectorFileStreamStore<float[]> finalDistancesStore = VectorFileIO.streamOut(FileType.xvec, float[].class, finalDistancesPath)
                     .orElseThrow(() -> new IOException("Could not create final distances file: " + finalDistancesPath))) {

                for (int queryIndex = 0; queryIndex < queryCount; queryIndex++) {
                    List<NeighborIndex> combined = new ArrayList<>(partitions.size() * Math.max(1, effectiveK));

                    for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
                        Iterator<int[]> neighborIterator = neighborIterators.get(partitionIndex);
                        Iterator<float[]> distanceIterator = distanceIterators.get(partitionIndex);

                        if (!neighborIterator.hasNext() || !distanceIterator.hasNext()) {
                            throw new IOException("Intermediate results ended unexpectedly for partition "
                                + partitions.get(partitionIndex).startIndex + "-" + partitions.get(partitionIndex).endIndex);
                        }

                        int[] neighborIndices = neighborIterator.next();
                        float[] distanceValues = distanceIterator.next();

                        if (neighborIndices.length != distanceValues.length) {
                            throw new IOException("Mismatched neighbor and distance counts in partition "
                                + partitions.get(partitionIndex).neighborsPath);
                        }

                        for (int i = 0; i < neighborIndices.length; i++) {
                            combined.add(new NeighborIndex(neighborIndices[i], distanceValues[i]));
                        }
                    }

                    Collections.sort(combined);
                    int resultSize = Math.min(effectiveK, combined.size());
                    int[] finalIndices = new int[resultSize];
                    float[] finalDistances = new float[resultSize];

                    for (int i = 0; i < resultSize; i++) {
                        NeighborIndex neighbor = combined.get(i);
                        finalIndices[i] = (int) neighbor.index();
                        finalDistances[i] = (float) neighbor.distance();
                    }

                    finalNeighborsStore.write(finalIndices);
                    finalDistancesStore.write(finalDistances);
                }
            }
        } finally {
            for (BoundedVectorFileStream<int[]> stream : neighborStreams) {
                try {
                    stream.close();
                } catch (Exception e) {
                    logger.warn("Error closing intermediate neighbors stream {}: {}", stream.getName(), e.getMessage());
                }
            }
            for (BoundedVectorFileStream<float[]> stream : distanceStreams) {
                try {
                    stream.close();
                } catch (Exception e) {
                    logger.warn("Error closing intermediate distances stream {}: {}", stream.getName(), e.getMessage());
                }
            }
        }
    }

    private Path buildIntermediatePath(Path baseOutputPath, int startIndex, int endIndex, String suffix, String extension) {
        String baseName = removeLastExtension(baseOutputPath.getFileName().toString());
        String fileName = String.format("%s.part_%06d_%06d.%s.%s", baseName, startIndex, endIndex, suffix, extension);
        Path parent = baseOutputPath.getParent();
        return parent != null ? parent.resolve(fileName) : Paths.get(fileName);
    }

    private Path deriveDistancesPath(Path neighborsPath) {
        String baseName = removeLastExtension(neighborsPath.getFileName().toString());
        String fileName = baseName + ".distances.fvec";
        Path parent = neighborsPath.getParent();
        return parent != null ? parent.resolve(fileName) : Paths.get(fileName);
    }

    private String removeLastExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx >= 0 ? filename.substring(0, idx) : filename;
    }

    private static class PartitionMetadata {
        final Path neighborsPath;
        final Path distancesPath;
        final int startIndex;
        final int endIndex;
        final int queryCount;
        final int partitionK;

        PartitionMetadata(Path neighborsPath, Path distancesPath, int startIndex, int endIndex, int queryCount, int partitionK) {
            this.neighborsPath = neighborsPath;
            this.distancesPath = distancesPath;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.queryCount = queryCount;
            this.partitionK = partitionK;
        }
    }

    public static void main(String[] args) {
        CMD_compute_knn cmd = new CMD_compute_knn();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        try {
            validatePaths();
        } catch (CommandLine.ParameterException e) {
            System.err.println(e.getMessage());
            return EXIT_ERROR;
        }

        if (k <= 0) {
            System.err.println("Error: Number of neighbors (k) must be positive");
            return EXIT_ERROR;
        }

        Path neighborsOutput = outputPath;
        Path distancesOutput = deriveDistancesPath(neighborsOutput);

        if (!force) {
            if (Files.exists(neighborsOutput)) {
                System.err.println("Error: Output neighbors file already exists. Use --force to overwrite.");
                return EXIT_FILE_EXISTS;
            }
            if (Files.exists(distancesOutput)) {
                System.err.println("Error: Output distances file already exists. Use --force to overwrite.");
                return EXIT_FILE_EXISTS;
            }
        }

        try {
            if (neighborsOutput.getParent() != null) {
                Files.createDirectories(neighborsOutput.getParent());
            }
            if (distancesOutput.getParent() != null) {
                Files.createDirectories(distancesOutput.getParent());
            }

            try (VectorFileArray<float[]> baseReader = VectorFileIO.randomAccess(FileType.xvec, float[].class, baseVectorsPath)) {
                int baseCount = baseReader.getSize();
                if (baseCount <= 0) {
                    System.err.println("Error: No base vectors found in file");
                    return EXIT_ERROR;
                }

                int baseDimension = baseReader.get(0).length;
                if (baseDimension <= 0) {
                    System.err.println("Error: Base vectors have zero dimension");
                    return EXIT_ERROR;
                }

                int effectiveK = Math.min(k, baseCount);
                int batchSize = Math.max(1, determineBatchSize(baseCount, baseDimension));
                int partitionCount = (int) Math.ceil((double) baseCount / batchSize);

                logger.info("compute knn: baseCount={}, baseDimension={}, effectiveK={}, batchSize={}, partitions={}",
                    baseCount, baseDimension, effectiveK, batchSize, partitionCount);

                List<PartitionMetadata> partitions = new ArrayList<>();
                int expectedQueryCount = -1;
                int partitionIndex = 0;

                for (int start = 0; start < baseCount; start += batchSize) {
                    int end = Math.min(baseCount, start + batchSize);
                    Path intermediateNeighbors = buildIntermediatePath(neighborsOutput, start, end, "neighbors", "ivec");
                    Path intermediateDistances = buildIntermediatePath(neighborsOutput, start, end, "distances", "fvec");

                    PartitionMetadata metadata = computePartition(
                        baseReader,
                        partitionIndex,
                        start,
                        end,
                        baseDimension,
                        intermediateNeighbors,
                        intermediateDistances,
                        effectiveK
                    );

                    if (expectedQueryCount < 0) {
                        expectedQueryCount = metadata.queryCount;
                    } else if (metadata.queryCount != expectedQueryCount) {
                        throw new IllegalStateException("Mismatch in query counts across partitions: expected "
                            + expectedQueryCount + " but partition " + partitionIndex + " produced " + metadata.queryCount);
                    }

                    partitions.add(metadata);
                    partitionIndex++;
                }

                if (expectedQueryCount < 0) {
                    System.err.println("Error: No query vectors found in file");
                    return EXIT_ERROR;
                }

                mergePartitions(partitions, neighborsOutput, distancesOutput, effectiveK, expectedQueryCount);

                System.out.println("Successfully computed KNN ground truth for " + expectedQueryCount + " query vectors");
                System.out.println("Base vectors: " + baseCount + ", k: " + effectiveK + ", distance metric: " + distanceMetric);
                System.out.println("Neighbors file: " + neighborsOutput);
                System.out.println("Distances file: " + distancesOutput);
                return EXIT_SUCCESS;
            }
        } catch (IOException e) {
            System.err.println("Error: I/O problem - " + e.getMessage());
            logger.error("I/O error during KNN computation", e);
            return EXIT_ERROR;
        } catch (Exception e) {
            System.err.println("Error computing KNN: " + e.getMessage());
            logger.error("Unexpected error during KNN computation", e);
            return EXIT_ERROR;
        }
    }
}
