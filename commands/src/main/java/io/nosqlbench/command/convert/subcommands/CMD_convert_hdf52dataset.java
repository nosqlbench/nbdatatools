package io.nosqlbench.command.convert.subcommands;

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

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.nbdatatools.api.fileio.VectorStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.spec.datasets.types.ViewKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.common.FlowStyle;
import picocli.CommandLine;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Enhanced subcommand to convert HDF5 datasets to vectordata dataset format.
 *
 * This version adds auto-scan functionality similar to merkle and catalog commands.
 * It can automatically scan directories for HDF5 files and convert those that don't
 * already have corresponding dataset directories.
 */
@CommandLine.Command(name = "hdf52dataset",
    header = "Convert HDF5 dataset to vectordata dataset format with auto-scan support",
    description = "Convert HDF5 files to vectordata dataset format with dataset.yaml and vector files. " +
                  "Can auto-scan directories for unconverted HDF5 files.",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_convert_hdf52dataset implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_convert_hdf52dataset.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_WARNING = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Parameters(description = "Input HDF5 files or directories to scan. " +
        "If no arguments provided, scans current directory.",
        arity = "0..*")
    private List<Path> inputs = new ArrayList<>();

    @CommandLine.Option(names = {"-o", "--output"},
        description = "Output dataset directory path (only used with single input file)")
    private Path outputPath;

    @CommandLine.Option(names = {"-f", "--force"},
        description = "Force overwrite if output directory already exists")
    private boolean force = false;

    @CommandLine.Option(names = {"--scan", "-s"},
        description = "Auto-scan directories for unconverted HDF5 files")
    private boolean scan = true;

    @CommandLine.Option(names = {"--dryrun", "-n"},
        description = "Show which files would be processed without actually converting")
    private boolean dryRun = false;

    @CommandLine.Option(names = {"--profile"},
        description = "Profile name for the dataset (default: 'default')",
        defaultValue = "default")
    private String profileName = "default";

    @CommandLine.Option(names = {"--distance"},
        description = "Distance function for the dataset (COSINE, EUCLIDEAN, DOT, etc.)",
        defaultValue = "COSINE")
    private String distanceFunction = "COSINE";

    @CommandLine.Option(names = {"-v", "--verbose"},
        description = "Enable verbose output")
    private boolean verbose = false;

    @CommandLine.Option(names = {"-q", "--quiet"},
        description = "Suppress all output except errors")
    private boolean quiet = false;

    @CommandLine.Option(names = {"--limit"},
        description = "Limit the number of vectors to convert per dataset")
    private Integer limit;

    @CommandLine.Option(names = {"--offset"},
        description = "Start converting from this vector index (0-based)",
        defaultValue = "0")
    private int offset = 0;

    @CommandLine.Option(names = {"--suffix"},
        description = "Suffix for generated dataset directories (default: '_dataset')",
        defaultValue = "_dataset")
    private String datasetSuffix = "_dataset";

    @CommandLine.Option(names = {"-h", "--help"},
        usageHelp = true,
        description = "Display this help message")
    private boolean helpRequested = false;

    @Override
    public Integer call() {
        try {
            // Default to current directory if no inputs provided
            if (inputs.isEmpty()) {
                inputs = List.of(Path.of("."));
            }

            // Collect all HDF5 files to process
            List<HDF5ConversionTask> tasks = new ArrayList<>();

            for (Path input : inputs) {
                if (!Files.exists(input)) {
                    logger.error("Input path does not exist: {}", input);
                    return EXIT_ERROR;
                }

                if (Files.isRegularFile(input)) {
                    // Single file mode
                    if (isHDF5File(input)) {
                        Path targetOutput = outputPath;
                        if (targetOutput == null) {
                            targetOutput = deriveDatasetPath(input);
                        }

                        if (shouldConvert(input, targetOutput)) {
                            tasks.add(new HDF5ConversionTask(input, targetOutput));
                        }
                    } else {
                        logger.warn("File is not an HDF5 file: {}", input);
                    }
                } else if (Files.isDirectory(input) && scan) {
                    // Directory scan mode
                    tasks.addAll(scanDirectoryForHDF5(input));
                }
            }

            if (tasks.isEmpty()) {
                if (!quiet) {
                    logger.info("No HDF5 files to convert");
                }
                return EXIT_SUCCESS;
            }

            // Report what will be done
            if (!quiet) {
                logger.info("Found {} HDF5 file(s) to process", tasks.size());
                if (verbose || dryRun) {
                    for (HDF5ConversionTask task : tasks) {
                        logger.info("  {} -> {}", task.inputPath, task.outputPath);
                    }
                }
            }

            if (dryRun) {
                logger.info("Dry-run mode: No files were actually converted");
                return EXIT_SUCCESS;
            }

            // Process each conversion task
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger skipCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            for (HDF5ConversionTask task : tasks) {
                try {
                    if (verbose) {
                        logger.info("Converting: {} -> {}", task.inputPath, task.outputPath);
                    }

                    int result = processHdf5File(task.inputPath, task.outputPath);

                    if (result == EXIT_SUCCESS) {
                        successCount.incrementAndGet();
                        if (!quiet) {
                            logger.info("Successfully converted: {}", task.inputPath.getFileName());
                        }
                    } else if (result == EXIT_WARNING) {
                        skipCount.incrementAndGet();
                    } else {
                        errorCount.incrementAndGet();
                    }

                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    logger.error("Error converting {}: {}", task.inputPath, e.getMessage());
                    if (verbose) {
                        logger.error("Stack trace:", e);
                    }
                }
            }

            // Summary
            if (!quiet) {
                logger.info("Conversion complete: {} succeeded, {} skipped, {} failed",
                    successCount.get(), skipCount.get(), errorCount.get());
            }

            if (errorCount.get() > 0) {
                return EXIT_ERROR;
            } else if (skipCount.get() > 0 && successCount.get() == 0) {
                return EXIT_WARNING;
            }

            return EXIT_SUCCESS;

        } catch (Exception e) {
            logger.error("Fatal error during conversion: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }

    /**
     * Scan a directory recursively for HDF5 files that need conversion
     */
    private List<HDF5ConversionTask> scanDirectoryForHDF5(Path directory) throws IOException {
        List<HDF5ConversionTask> tasks = new ArrayList<>();

        Files.walkFileTree(directory, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
            new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    // Skip if this is inside a dataset directory
                    if (isInsideDatasetDirectory(file)) {
                        return FileVisitResult.CONTINUE;
                    }

                    if (isHDF5File(file)) {
                        Path datasetPath = deriveDatasetPath(file);

                        if (shouldConvert(file, datasetPath)) {
                            tasks.add(new HDF5ConversionTask(file, datasetPath));
                            if (verbose) {
                                logger.debug("Found unconverted HDF5 file: {}", file);
                            }
                        } else if (verbose) {
                            logger.debug("Skipping already converted: {}", file);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    // Skip directories that are dataset directories
                    if (Files.exists(dir.resolve("dataset.yaml"))) {
                        if (verbose) {
                            logger.debug("Skipping dataset directory: {}", dir);
                        }
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });

        return tasks;
    }

    /**
     * Check if a file is inside a dataset directory (has dataset.yaml)
     */
    private boolean isInsideDatasetDirectory(Path file) {
        Path parent = file.getParent();
        while (parent != null) {
            if (Files.exists(parent.resolve("dataset.yaml"))) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    /**
     * Check if a file is an HDF5 file based on extension
     */
    private boolean isHDF5File(Path file) {
        String fileName = file.getFileName().toString().toLowerCase();
        return fileName.endsWith(".hdf5") || fileName.endsWith(".h5");
    }

    /**
     * Derive the dataset directory path from an HDF5 file path
     */
    private Path deriveDatasetPath(Path hdf5File) {
        String fileName = hdf5File.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        String baseName = dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;

        return hdf5File.getParent().resolve(baseName + datasetSuffix);
    }

    /**
     * Check if an HDF5 file should be converted
     */
    private boolean shouldConvert(Path hdf5File, Path datasetPath) {
        if (!Files.exists(datasetPath)) {
            return true;
        }

        if (force) {
            return true;
        }

        // Check if dataset.yaml exists
        Path datasetYaml = datasetPath.resolve("dataset.yaml");
        if (!Files.exists(datasetYaml)) {
            return true;
        }

        // Check for incomplete conversions - must have at least one data file
        boolean hasDataFiles = false;
        try {
            hasDataFiles = Files.list(datasetPath)
                .anyMatch(p -> {
                    String name = p.getFileName().toString();
                    return name.endsWith(".fvec") || name.endsWith(".ivec") ||
                           name.endsWith(".bvec") || name.endsWith(".dvec");
                });
        } catch (IOException e) {
            // If we can't check, assume incomplete
            return true;
        }

        if (!hasDataFiles) {
            if (verbose) {
                logger.info("Dataset directory {} exists but has no data files, will re-convert", datasetPath);
            }
            return true;
        }

        return false;
    }

    private Integer processHdf5File(Path hdf5Path, Path outputDir) {
        try {
            // Skip if already exists and not forcing
            if (Files.exists(outputDir) && !force) {
                if (!quiet) {
                    logger.debug("Dataset already exists, skipping: {}", outputDir);
                }
                return EXIT_WARNING;
            }

            // Create output directory
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
            }

            if (verbose) {
                logger.info("Opening HDF5 file: {}", hdf5Path);
            }

            try (HdfFile hdfFile = new HdfFile(hdf5Path)) {
                // Discover datasets in the HDF5 file
                Map<String, DatasetInfo> datasets = discoverDatasets(hdfFile);

                if (datasets.isEmpty()) {
                    logger.warn("No datasets found in HDF5 file: {}", hdf5Path);
                    return EXIT_WARNING;
                }

                if (verbose) {
                    logger.info("Found {} datasets in HDF5 file", datasets.size());
                    for (Map.Entry<String, DatasetInfo> entry : datasets.entrySet()) {
                        logger.info("  Dataset '{}': type={}, shape={}",
                            entry.getKey(), entry.getValue().dataType,
                            formatShape(entry.getValue().shape));
                    }
                }

                // Convert each dataset
                Map<String, String> convertedFiles = new HashMap<>();
                logger.info("Starting conversion of {} datasets", datasets.size());
                for (Map.Entry<String, DatasetInfo> entry : datasets.entrySet()) {
                    String name = entry.getKey();
                    DatasetInfo info = entry.getValue();

                    if (verbose) {
                        logger.info("Processing dataset '{}': type={}, shape={}",
                            name, info.dataType, formatShape(info.shape));
                    }

                    try {
                        String outputFileName = convertDataset(info, outputDir);
                        if (outputFileName != null) {
                            convertedFiles.put(name, outputFileName);
                            logger.info("Converted dataset '{}' to {}", name, outputFileName);
                        } else {
                            logger.info("Skipped dataset '{}' (unsupported type or structure)", name);
                        }
                    } catch (Exception e) {
                        logger.error("Error converting dataset '{}': {}", name, e.getMessage(), e);
                    }
                }

                if (convertedFiles.isEmpty()) {
                    logger.warn("No datasets could be converted from: {}", hdf5Path);
                    return EXIT_WARNING;
                }

                // Create dataset.yaml
                createDatasetYaml(outputDir, convertedFiles, hdf5Path);

                return EXIT_SUCCESS;

            } catch (Exception e) {
                logger.error("Error processing HDF5 file {}: {}", hdf5Path, e.getMessage());
                if (verbose) {
                    logger.error("Stack trace:", e);
                }
                return EXIT_ERROR;
            }

        } catch (IOException e) {
            logger.error("IO error processing {}: {}", hdf5Path, e.getMessage());
            return EXIT_ERROR;
        }
    }

    private Map<String, DatasetInfo> discoverDatasets(HdfFile hdfFile) {
        Map<String, DatasetInfo> datasets = new LinkedHashMap<>();
        discoverDatasetsRecursive(hdfFile, "", datasets);
        return datasets;
    }

    private void discoverDatasetsRecursive(Group group, String prefix, Map<String, DatasetInfo> datasets) {
        logger.debug("Discovering datasets in group: {}, prefix: {}", group.getName(), prefix);
        for (Node node : group) {
            String nodeName = prefix.isEmpty() ? node.getName() : prefix + "/" + node.getName();
            logger.debug("Found node: {}, type: {}", nodeName, node.getClass().getSimpleName());

            if (node instanceof Dataset) {
                Dataset dataset = (Dataset) node;
                DatasetInfo info = new DatasetInfo();
                info.dataset = dataset;
                info.dataType = dataset.getDataType().getJavaType();
                info.shape = dataset.getDimensions();

                String simpleName = determineOutputName(nodeName);
                info.name = simpleName;  // Use the mapped name for the output file
                datasets.put(simpleName, info);

            } else if (node instanceof Group) {
                discoverDatasetsRecursive((Group) node, nodeName, datasets);
            }
        }
    }

    private String determineOutputName(String hdf5Path) {
        String lowerPath = hdf5Path.toLowerCase();
        String name = hdf5Path;

        if (name.startsWith("/")) {
            name = name.substring(1);
        }
        name = name.replace("/", "_");

        // Map common names to standard vectordata names
        if (lowerPath.contains("train") || lowerPath.contains("base")) {
            return ViewKind.base.name();
        } else if (lowerPath.contains("query") || lowerPath.contains("test")) {
            return ViewKind.query.name();
        } else if (lowerPath.contains("neighbor") && lowerPath.contains("indices")) {
            return ViewKind.indices.name();
        } else if (lowerPath.contains("neighbor") && lowerPath.contains("distance")) {
            return ViewKind.neighbors.name();
        } else if (lowerPath.contains("indices") || lowerPath.contains("idx")) {
            return ViewKind.indices.name();
        } else if (lowerPath.contains("distance") || lowerPath.contains("dist")) {
            return ViewKind.neighbors.name();
        }

        return name;
    }

    private String convertDataset(DatasetInfo info, Path outputDir) throws IOException {
        Class<?> dataType = info.dataType;

        String extension;
        FileType fileType;

        if (dataType == float.class || dataType == float[].class ||
            dataType == float[][].class) {
            extension = ".fvec";
            fileType = FileType.xvec;
        } else if (dataType == int.class || dataType == int[].class ||
                   dataType == int[][].class) {
            extension = ".ivec";
            fileType = FileType.xvec;
        } else if (dataType == byte.class || dataType == byte[].class ||
                   dataType == byte[][].class) {
            extension = ".bvec";
            fileType = FileType.xvec;
        } else if (dataType == double.class || dataType == double[].class ||
                   dataType == double[][].class) {
            extension = ".fvec";
            fileType = FileType.xvec;
        } else {
            if (verbose) {
                logger.debug("Unsupported data type for dataset '{}': {}", info.name, dataType);
            }
            return null;
        }

        String outputFileName = info.name + extension;
        Path outputPath = outputDir.resolve(outputFileName);

        if (verbose) {
            logger.info("Writing {} to {}", info.name, outputPath);
        }

        if (info.shape.length == 1) {
            convertSimpleArray(info, outputPath, fileType);
        } else if (info.shape.length == 2) {
            convert2DArray(info, outputPath, fileType);
        } else {
            if (verbose) {
                logger.debug("Unsupported array dimensionality for dataset '{}': {}",
                    info.name, info.shape.length);
            }
            return null;
        }

        if (!quiet) {
            logger.info("Created: {}", outputPath.getFileName());
        }

        return outputFileName;
    }

    private void convertSimpleArray(DatasetInfo info, Path outputPath, FileType fileType) throws IOException {
        Object data = info.dataset.getData();

        Optional<? extends VectorStreamStore<float[]>> writerOpt =
            VectorFileIO.streamOut(fileType, float[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<float[]> writer = writerOpt.get()) {

            float[] vector = convertToFloatArray(data);
            writer.write(vector);
        } catch (Exception e) {
            throw new IOException("Error writing vector data", e);
        }
    }

    private void convert2DArray(DatasetInfo info, Path outputPath, FileType fileType) throws IOException {
        Object data = info.dataset.getData();
        Class<?> dataType = info.dataType;

        // Check if data is null
        if (data == null) {
            throw new IOException("Dataset '" + info.name + "' returned null data");
        }

        int numVectors = info.shape[0];
        int dimensions = info.shape[1];

        int startIdx = Math.min(offset, numVectors);
        int endIdx = limit != null ? Math.min(startIdx + limit, numVectors) : numVectors;

        logger.info("Converting vectors {} to {} (dimensions: {}) for dataset '{}'",
            startIdx, endIdx - 1, dimensions, info.name);
        logger.info("Data type: {}, Data class: {}, Data is null: {}",
            dataType, data.getClass(), data == null);

        // Check actual data class instead of dataType for array matching
        Class<?> actualDataClass = data.getClass();
        logger.info("Checking data class: actual={}, float[][]={}, equals={}",
            actualDataClass, float[][].class, actualDataClass == float[][].class);

        if (actualDataClass == float[][].class) {
            logger.info("Writing float vectors");
            writeFloatVectors((float[][]) data, outputPath, startIdx, endIdx);
        } else if (actualDataClass == double[][].class) {
            logger.info("Writing double vectors as float");
            writeDoubleVectorsAsFloat(data, outputPath, startIdx, endIdx, dimensions);
        } else if (actualDataClass == int[][].class) {
            logger.info("Writing int vectors");
            writeIntVectors(data, outputPath, startIdx, endIdx, dimensions);
        } else if (actualDataClass == byte[][].class) {
            logger.info("Writing byte vectors");
            writeByteVectors(data, outputPath, startIdx, endIdx, dimensions);
        } else if (actualDataClass == long[][].class) {
            logger.info("Writing long vectors as int");
            writeLongVectorsAsInt(data, outputPath, startIdx, endIdx, dimensions);
        } else {
            logger.warn("Unsupported data type for 2D array: dataType={}, actualClass={}",
                dataType, actualDataClass);
        }
    }

    private void writeFloatVectors(float[][] vectors, Path outputPath, int startIdx, int endIdx)
        throws IOException {
        logger.info("Writing {} float vectors to {}", endIdx - startIdx, outputPath);
        logger.info("Vectors array length: {}, First vector length: {}",
            vectors.length, vectors.length > 0 ? vectors[0].length : "N/A");

        Optional<? extends VectorStreamStore<float[]>> writerOpt =
            VectorFileIO.streamOut(FileType.xvec, float[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<float[]> writer = writerOpt.get()) {
            int written = 0;
            for (int i = startIdx; i < endIdx; i++) {
                if (vectors[i] == null) {
                    logger.error("Vector at index {} is null!", i);
                    continue;
                }
                writer.write(vectors[i]);
                written++;
                if (written == 1) {
                    logger.info("Wrote first vector with length {}", vectors[i].length);
                }
            }
            // writer.flush();  // VectorStreamStore may not have flush
            logger.info("Successfully wrote {} vectors", written);

            // Verify file exists after writing
            if (!Files.exists(outputPath)) {
                throw new IOException("File was not created: " + outputPath);
            }
            long fileSize = Files.size(outputPath);
            logger.info("Created file {} with size {} bytes", outputPath, fileSize);
        } catch (Exception e) {
            logger.error("Failed to write float vectors to {}", outputPath, e);
            throw new IOException("Error writing float vectors to " + outputPath, e);
        }
    }

    private void writeDoubleVectorsAsFloat(Object data, Path outputPath, int startIdx, int endIdx, int dimensions)
        throws IOException {
        Optional<? extends VectorStreamStore<float[]>> writerOpt =
            VectorFileIO.streamOut(FileType.xvec, float[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<float[]> writer = writerOpt.get()) {
            if (data instanceof double[][]) {
                double[][] vectors = (double[][]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    float[] floatVector = new float[dimensions];
                    for (int j = 0; j < dimensions; j++) {
                        floatVector[j] = (float) vectors[i][j];
                    }
                    writer.write(floatVector);
                }
            } else if (data instanceof double[]) {
                double[] flatData = (double[]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    float[] floatVector = new float[dimensions];
                    int baseIdx = i * dimensions;
                    for (int j = 0; j < dimensions; j++) {
                        floatVector[j] = (float) flatData[baseIdx + j];
                    }
                    writer.write(floatVector);
                }
            }
        } catch (Exception e) {
            throw new IOException("Error writing double vectors as float", e);
        }
    }

    private void writeIntVectors(Object data, Path outputPath, int startIdx, int endIdx, int dimensions)
        throws IOException {
        Optional<? extends VectorStreamStore<int[]>> writerOpt =
            VectorFileIO.streamOut(FileType.xvec, int[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<int[]> writer = writerOpt.get()) {
            if (data instanceof int[][]) {
                int[][] vectors = (int[][]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    writer.write(vectors[i]);
                }
            } else if (data instanceof int[]) {
                int[] flatData = (int[]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    int[] vector = new int[dimensions];
                    int baseIdx = i * dimensions;
                    System.arraycopy(flatData, baseIdx, vector, 0, dimensions);
                    writer.write(vector);
                }
            }
        } catch (Exception e) {
            throw new IOException("Error writing int vectors", e);
        }
    }

    private void writeLongVectorsAsInt(Object data, Path outputPath, int startIdx, int endIdx, int dimensions)
        throws IOException {
        Optional<? extends VectorStreamStore<int[]>> writerOpt =
            VectorFileIO.streamOut(FileType.xvec, int[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<int[]> writer = writerOpt.get()) {
            long[][] vectors = (long[][]) data;
            for (int i = startIdx; i < endIdx; i++) {
                int[] intVector = new int[dimensions];
                for (int j = 0; j < dimensions; j++) {
                    intVector[j] = (int) vectors[i][j];
                }
                writer.write(intVector);
            }
        } catch (Exception e) {
            throw new IOException("Error writing long vectors as int", e);
        }
    }

    private void writeByteVectors(Object data, Path outputPath, int startIdx, int endIdx, int dimensions)
        throws IOException {
        Optional<? extends VectorStreamStore<byte[]>> writerOpt =
            VectorFileIO.streamOut(FileType.xvec, byte[].class, outputPath);

        if (!writerOpt.isPresent()) {
            throw new IOException("Failed to create writer for " + outputPath);
        }

        try (VectorStreamStore<byte[]> writer = writerOpt.get()) {
            if (data instanceof byte[][]) {
                byte[][] vectors = (byte[][]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    writer.write(vectors[i]);
                }
            } else if (data instanceof byte[]) {
                byte[] flatData = (byte[]) data;
                for (int i = startIdx; i < endIdx; i++) {
                    byte[] vector = new byte[dimensions];
                    int baseIdx = i * dimensions;
                    System.arraycopy(flatData, baseIdx, vector, 0, dimensions);
                    writer.write(vector);
                }
            }
        } catch (Exception e) {
            throw new IOException("Error writing byte vectors", e);
        }
    }

    private float[] convertToFloatArray(Object data) {
        if (data instanceof float[]) {
            return (float[]) data;
        } else if (data instanceof double[]) {
            double[] doubles = (double[]) data;
            float[] floats = new float[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                floats[i] = (float) doubles[i];
            }
            return floats;
        } else if (data instanceof int[]) {
            int[] ints = (int[]) data;
            float[] floats = new float[ints.length];
            for (int i = 0; i < ints.length; i++) {
                floats[i] = (float) ints[i];
            }
            return floats;
        } else if (data instanceof byte[]) {
            byte[] bytes = (byte[]) data;
            float[] floats = new float[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                floats[i] = (float) (bytes[i] & 0xFF);
            }
            return floats;
        }
        throw new IllegalArgumentException("Cannot convert data type to float array: " + data.getClass());
    }

    private void createDatasetYaml(Path outputDir, Map<String, String> files, Path sourcePath) throws IOException {
        Path yamlPath = outputDir.resolve("dataset.yaml");

        Map<String, Object> yamlData = new LinkedHashMap<>();

        // Add attributes
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("distance_function", distanceFunction);
        attributes.put("created_by", "convert hdf52dataset");
        attributes.put("source", sourcePath.getFileName().toString());
        attributes.put("conversion_date", new Date().toString());
        yamlData.put("attributes", attributes);

        // Add profile
        Map<String, Object> profiles = new LinkedHashMap<>();
        Map<String, String> profile = new LinkedHashMap<>();

        // Map converted files to standard profile fields
        for (Map.Entry<String, String> entry : files.entrySet()) {
            String key = entry.getKey();
            String fileName = entry.getValue();

            ViewKind.fromName(key).ifPresentOrElse(
                kind -> profile.put(kind.name(), fileName),
                () -> profile.put(key, fileName)
            );
        }

        profiles.put(profileName, profile);
        yamlData.put("profiles", profiles);

        // Write YAML file
        DumpSettings settings = DumpSettings.builder()
            .setDefaultFlowStyle(FlowStyle.BLOCK)
            .build();

        Dump dump = new Dump(settings);
        String yamlString = dump.dumpToString(yamlData);

        try (FileWriter writer = new FileWriter(yamlPath.toFile())) {
            writer.write(yamlString);
        }

        if (verbose) {
            logger.debug("Created dataset.yaml file in {}", outputDir);
        }
    }

    private String formatShape(int[] shape) {
        if (shape == null || shape.length == 0) {
            return "[]";
        }
        return Arrays.stream(shape)
            .mapToObj(String::valueOf)
            .collect(Collectors.joining(", ", "[", "]"));
    }

    private static class DatasetInfo {
        String name;
        Dataset dataset;
        Class<?> dataType;
        int[] shape;
    }

    private static class HDF5ConversionTask {
        final Path inputPath;
        final Path outputPath;

        HDF5ConversionTask(Path inputPath, Path outputPath) {
            this.inputPath = inputPath;
            this.outputPath = outputPath;
        }
    }
}
