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

import io.nosqlbench.command.common.VectorDataCompletionCandidates;
import io.nosqlbench.command.common.VectorDataSpec;
import io.nosqlbench.command.common.VectorDataSpecSupport;
import io.nosqlbench.command.common.VectorDataSpecConverter;
import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Describe the contents of a vector file or dataset facet.
///
/// This command uses the unified VectorDataSpec format to describe:
/// - Local files: `file:./vectors.fvec` or `./vectors.fvec`
/// - Catalog facets: `facet.dataset.profile.facet`
/// - Local directory facets: `facet:./mydir:profile:facet` (use ':' for paths with dots)
/// - Remote files: `https://example.com/vectors.fvec`
///
/// For facets, the facet name can be: base, query, indices, distances
/// (or full names: base_vectors, query_vectors, neighbor_indices, neighbor_distances)
@CommandLine.Command(name = "describe",
    header = "Describe the contents of a vector file or dataset facet",
    description = "Provides information about the data in a vector file or dataset facet, " +
        "including dimensions and vector count.\n\n" +
        "The --vectors parameter accepts:\n" +
        "  - Local file: ./data/vectors.fvec or file:./data/vectors.fvec\n" +
        "  - Catalog facet: facet.dataset.profile.facet\n" +
        "  - Local facet: facet:./mydir:profile:facet\n" +
        "  - Remote file: https://example.com/vectors.fvec\n\n" +
        "Facet names: base, query, indices, distances",
    exitCodeList = {"0: success", "1: error processing source"})
public class CMD_analyze_describe implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_describe.class);

    @CommandLine.Parameters(paramLabel = "VECTORS",
        description = "Vector data source (file path, facet.dataset.profile.facet, or URL)",
        arity = "1",
        converter = VectorDataSpecConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class)
    private VectorDataSpec vectors;

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--cache-dir"},
        description = "Directory for cached dataset files")
    private Path cacheDir;

    /// Execute the command to describe the specified vector data source
    ///
    /// @return 0 for success, 1 for error
    @Override
    public Integer call() {
        try {
            this.configdir = expandPath(this.configdir);

            System.out.println("Analyzing: " + vectors.toDescription());
            System.out.println();

            return switch (vectors.getSourceType()) {
                case LOCAL_FILE -> describeLocalFile(vectors.getLocalPath().orElseThrow());
                case LOCAL_FACET -> describeLocalFacet();
                case CATALOG_FACET -> describeCatalogFacet();
                case REMOTE_FILE -> describeRemoteFile();
                case REMOTE_DATASET_YAML -> describeRemoteDataset();
            };
        } catch (Exception e) {
            logger.error("Error processing source: {}", e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Stack trace:", e);
            }
            return 1;
        }
    }

    /// Describe a facet from the catalog
    private int describeCatalogFacet() {
        String datasetName = vectors.getDatasetRef().orElseThrow();
        String profileName = vectors.getProfileName().orElseThrow();
        TestDataKind facetKind = vectors.getFacetKind().orElseThrow();

        try {
            TestDataSources config = new TestDataSources().configure(this.configdir);
            if (this.catalogs != null && !this.catalogs.isEmpty()) {
                config = config.addCatalogs(this.catalogs);
            }

            Catalog catalog = Catalog.of(config);
            DatasetProfileSpec datasetSpec = DatasetProfileSpec.parse(datasetName + ":" + profileName);

            ProfileSelector profileSelector = catalog.select(datasetSpec);
            Path resolvedCacheDir = VectorDataSpecSupport.requireCacheDir(cacheDir);
            profileSelector = profileSelector.setCacheDir(resolvedCacheDir.toString());

            TestDataView testDataView = profileSelector.profile(profileName);
            return describeFacet(testDataView, facetKind);

        } catch (Exception e) {
            logger.error("Failed to resolve catalog facet '{}:{}:{}': {}",
                datasetName, profileName, facetKind.name(), e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Stack trace:", e);
            }
            return 1;
        }
    }

    /// Describe a facet from a local directory containing dataset.yaml
    private int describeLocalFacet() {
        Path datasetDir = vectors.getLocalPath().orElseThrow();
        String profileName = vectors.getProfileName().orElseThrow();
        TestDataKind facetKind = vectors.getFacetKind().orElseThrow();

        try {
            // Create a catalog from the local directory
            TestDataSources config = new TestDataSources().configure(this.configdir);
            config = config.addCatalogs(List.of(datasetDir.toString()));

            Catalog catalog = Catalog.of(config);

            // The dataset name is typically the directory name
            String datasetName = datasetDir.getFileName().toString();
            DatasetProfileSpec datasetSpec = DatasetProfileSpec.parse(datasetName + ":" + profileName);

            ProfileSelector profileSelector = catalog.select(datasetSpec);
            Path resolvedCacheDir = VectorDataSpecSupport.requireCacheDir(cacheDir);
            profileSelector = profileSelector.setCacheDir(resolvedCacheDir.toString());

            TestDataView testDataView = profileSelector.profile(profileName);
            return describeFacet(testDataView, facetKind);

        } catch (Exception e) {
            logger.error("Failed to resolve local facet in '{}': {}", datasetDir, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Stack trace:", e);
            }
            return 1;
        }
    }

    /// Describe a remote file (not yet implemented - placeholder)
    private int describeRemoteFile() {
        logger.error("Remote file describe is not yet implemented for: {}", vectors.getRemoteUri().orElse(null));
        return 1;
    }

    /// Describe a remote dataset (not yet implemented - placeholder)
    private int describeRemoteDataset() {
        logger.error("Remote dataset describe is not yet implemented for: {}", vectors.getRemoteUri().orElse(null));
        return 1;
    }

    /// Describe a specific facet from a TestDataView
    private int describeFacet(TestDataView view, TestDataKind facetKind) {
        switch (facetKind) {
            case base_vectors -> {
                Optional<BaseVectors> baseVectors = view.getBaseVectors();
                if (baseVectors.isEmpty()) {
                    logger.error("No base_vectors facet available in this dataset profile");
                    return 1;
                }
                describeDatasetView("base_vectors", baseVectors.get(), "float");
            }
            case query_vectors -> {
                Optional<QueryVectors> queryVectors = view.getQueryVectors();
                if (queryVectors.isEmpty()) {
                    logger.error("No query_vectors facet available in this dataset profile");
                    return 1;
                }
                describeDatasetView("query_vectors", queryVectors.get(), "float");
            }
            case neighbor_indices -> {
                Optional<NeighborIndices> neighborIndices = view.getNeighborIndices();
                if (neighborIndices.isEmpty()) {
                    logger.error("No neighbor_indices facet available in this dataset profile");
                    return 1;
                }
                describeNeighborIndices(neighborIndices.get());
            }
            case neighbor_distances -> {
                Optional<NeighborDistances> neighborDistances = view.getNeighborDistances();
                if (neighborDistances.isEmpty()) {
                    logger.error("No neighbor_distances facet available in this dataset profile");
                    return 1;
                }
                describeDatasetView("neighbor_distances", neighborDistances.get(), "float");
            }
            default -> {
                logger.error("Facet '{}' is not supported for describe command", facetKind.name());
                return 1;
            }
        }
        return 0;
    }

    /// Describe a generic DatasetView
    private void describeDatasetView(String facetName, DatasetView<?> datasetView, String dataType) {
        System.out.println("Dataset Facet Description:");
        System.out.printf("- Facet: %s%n", facetName);
        System.out.printf("- Data Type: %s%n", dataType);
        System.out.printf("- Dimensions: %d%n", datasetView.getVectorDimensions());
        System.out.printf("- Vector Count: %d%n", datasetView.getCount());

        int recordSize = 4 + (datasetView.getVectorDimensions() * 4); // 4 bytes for dim + float data
        System.out.printf("- Record Size: %d bytes%n", recordSize);
    }

    /// Describe neighbor indices specifically (includes maxK)
    private void describeNeighborIndices(NeighborIndices neighborIndices) {
        System.out.println("Dataset Facet Description:");
        System.out.printf("- Facet: neighbor_indices%n");
        System.out.printf("- Data Type: int%n");
        System.out.printf("- Dimensions (k): %d%n", neighborIndices.getVectorDimensions());
        System.out.printf("- Vector Count: %d%n", neighborIndices.getCount());
        System.out.printf("- Max K: %d%n", neighborIndices.getMaxK());

        int recordSize = 4 + (neighborIndices.getVectorDimensions() * 4); // 4 bytes for dim + int data
        System.out.printf("- Record Size: %d bytes%n", recordSize);
    }

    /// Describe a local file
    private int describeLocalFile(Path file) {
        try {
            if (!Files.exists(file)) {
                logger.error("File not found: {}", file);
                return 1;
            }

            String fileExtension = getFileExtension(file);

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

    /// Expand path variables like ~ and ${HOME}
    private Path expandPath(Path path) {
        return Path.of(path.toString()
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home")));
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
        int elementSize = switch (dataTypeStr.toLowerCase()) {
            case "float" -> 4;  // 4 bytes per float
            case "int" -> 4;    // 4 bytes per int
            case "double" -> 8; // 8 bytes per double
            default -> 4;       // Default to 4 bytes
        };

        // Calculate record size based on file type
        return switch (fileType) {
            case xvec -> 4 + (dimensions * elementSize); // 4 bytes for dimension + data
            case parquet, csv -> dimensions * elementSize; // Parquet/CSV have their own overhead
            default -> dimensions * elementSize;
        };
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

                if (vector instanceof float[] floatVector) {
                    dimensions = floatVector.length;
                    dataTypeStr = "float";
                } else if (vector instanceof int[] intVector) {
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
