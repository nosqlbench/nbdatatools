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
import io.nosqlbench.readers.ReaderUtils;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.merklev2.CacheFileAccessor;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.CoreXVecVectorDatasetViewMethods;
import io.nosqlbench.vectordata.spec.datasets.types.VectorDatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Check that xvec header dimensions use the expected little-endian layout
@CommandLine.Command(name = "check-endian",
    header = "Verify xvec files use little-endian dimension headers",
    description = "Inspects xvec files or dataset facets to ensure vector counts are encoded with the expected little-endian order",
    exitCodeList = {"0: success", "1: validation error"})
public class CMD_analyze_check_endian implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_analyze_check_endian.class);

    @CommandLine.Parameters(arity = "1..*", paramLabel = "VECTORS",
        description = "One or more vector sources (xvec files or dataset facets) to inspect",
        converter = VectorDataSpecConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class)
    private List<VectorDataSpec> vectors = new ArrayList<>();

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

    @Override
    public Integer call() {
        this.configdir = expandPath(this.configdir);

        boolean hadError = false;
        for (VectorDataSpec spec : vectors) {
            if (inspectSpec(spec)) {
                hadError = true;
            }
        }

        return hadError ? 1 : 0;
    }

    private int resolveElementWidth(Path file) {
        String name = file.getFileName().toString().toLowerCase();
        if (name.endsWith(".fvec") || name.endsWith(".fvecs")
            || name.endsWith(".ivec") || name.endsWith(".ivecs")
            || name.endsWith(".bvec") || name.endsWith(".bvecs")
            || name.endsWith(".xvec") || name.endsWith(".xvecs")) {
            return 4;
        }
        if (name.endsWith(".dvec") || name.endsWith(".dvecs")) {
            return 8;
        }
        return -1;
    }

    private boolean inspectSpec(VectorDataSpec spec) {
        return switch (spec.getSourceType()) {
            case LOCAL_FILE -> inspectLocalFile(spec);
            case LOCAL_FACET, CATALOG_FACET -> inspectFacet(spec);
            case REMOTE_FILE, REMOTE_DATASET_YAML -> {
                System.err.printf("  Error: Remote specs are not supported for endianness checks: %s%n", spec);
                yield true;
            }
        };
    }

    private boolean inspectLocalFile(VectorDataSpec spec) {
        Path file = spec.getLocalPath().orElseThrow();
        String label = file.toString();
        System.out.printf("Analyzing %s%n", label);

        if (!Files.exists(file)) {
            System.err.printf("  Error: File not found. Please verify the path and try again.%n");
            return true;
        }

        int elementWidth = resolveElementWidth(file);
        if (elementWidth <= 0) {
            System.err.printf("  Error: Unsupported file extension. Expected .fvec/.ivec/.bvec/.dvec variants.%n");
            return true;
        }

        try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(file, StandardOpenOption.READ)) {
            return inspectChannel(label, channel, elementWidth);
        } catch (IOException e) {
            System.err.printf("  Error: Failed to inspect %s: %s%n", file, e.getMessage());
            System.err.println("  Suggestion: Ensure the file is readable and in a supported format.");
            return true;
        }
    }

    private boolean inspectFacet(VectorDataSpec spec) {
        String label = spec.toDescription();
        System.out.printf("Analyzing %s%n", label);

        Optional<VectorTestDataView> view = resolveFacetView(spec);
        if (view.isEmpty()) {
            return true;
        }

        TestDataKind facetKind = spec.getFacetKind().orElseThrow();
        Optional<VectorDatasetView<?>> datasetView = resolveDatasetView(view.get(), facetKind);
        if (datasetView.isEmpty()) {
            System.err.printf("  Error: Facet '%s' is not available for %s.%n", facetKind.name(), spec);
            return true;
        }

        int elementWidth = resolveElementWidth(datasetView.get());
        if (elementWidth <= 0) {
            System.err.printf("  Error: Unsupported data type for facet '%s'.%n", facetKind.name());
            return true;
        }

        if (!(datasetView.get() instanceof CoreXVecVectorDatasetViewMethods<?> xvecView)) {
            System.err.printf("  Error: Facet '%s' is not backed by an xvec file.%n", facetKind.name());
            return true;
        }

        return inspectChannel(label, xvecView.getChannel(), elementWidth);
    }

    private Optional<VectorTestDataView> resolveFacetView(VectorDataSpec spec) {
        String profileName = spec.getProfileName().orElseThrow();

        try {
            TestDataSources config = new TestDataSources().configure(this.configdir);
            if (spec.isCatalogFacet() && this.catalogs != null && !this.catalogs.isEmpty()) {
                config = config.addCatalogs(this.catalogs);
            }

            Catalog catalog;
            String datasetName;
            if (spec.isLocalFacet()) {
                Path datasetDir = spec.getLocalPath().orElseThrow();
                config = config.addCatalogs(List.of(datasetDir.toString()));
                catalog = Catalog.of(config);
                datasetName = datasetDir.getFileName().toString();
            } else {
                catalog = Catalog.of(config);
                datasetName = spec.getDatasetRef().orElseThrow();
            }

            DatasetProfileSpec datasetSpec = DatasetProfileSpec.parse(datasetName + ":" + profileName);
            ProfileSelector profileSelector = catalog.select(datasetSpec);
            Path resolvedCacheDir = VectorDataSpecSupport.requireCacheDir(cacheDir);
            profileSelector = profileSelector.setCacheDir(resolvedCacheDir.toString());

            return Optional.of(profileSelector.profile(profileName));
        } catch (Exception e) {
            logger.error("Failed to resolve facet '{}': {}", spec, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Stack trace:", e);
            }
            System.err.printf("  Error: Unable to resolve dataset facet for '%s'.%n", spec);
            return Optional.empty();
        }
    }

    private Optional<VectorDatasetView<?>> resolveDatasetView(VectorTestDataView view, TestDataKind facetKind) {
        return switch (facetKind) {
            case base_vectors -> view.getBaseVectors().map(v -> (VectorDatasetView<?>) v);
            case query_vectors -> view.getQueryVectors().map(v -> (VectorDatasetView<?>) v);
            case neighbor_indices -> view.getNeighborIndices().map(v -> (VectorDatasetView<?>) v);
            case neighbor_distances -> view.getNeighborDistances().map(v -> (VectorDatasetView<?>) v);
            default -> Optional.empty();
        };
    }

    private int resolveElementWidth(VectorDatasetView<?> vectorDatasetView) {
        Class<?> dataType = vectorDatasetView.getDataType();
        if (dataType.isArray()) {
            dataType = dataType.getComponentType();
        }
        if (dataType == float.class || dataType == int.class) {
            return 4;
        }
        if (dataType == byte.class) {
            return 1;
        }
        if (dataType == double.class || dataType == long.class) {
            return 8;
        }
        if (dataType == short.class) {
            return 2;
        }
        return -1;
    }

    private boolean inspectChannel(String displayLabel, AsynchronousFileChannel channel, int elementWidth) {
        String sourceLabel = displayLabel;
        if (channel instanceof CacheFileAccessor accessor) {
            Path cachePath = accessor.getCacheFilePath();
            if (cachePath != null) {
                sourceLabel = cachePath.toString();
                System.out.printf("  Cache file: %s%n", cachePath);
            }
        }

        System.out.printf("  Step 1: Element width detected as %d bytes%n", elementWidth);

        try {
            System.out.println("  Step 2: Evaluating header endianness assumptions...");
            ReaderUtils.EndianCheckResult result = ReaderUtils.checkXvecEndianness(channel, sourceLabel, elementWidth);

            if (result.isEndianMismatch()) {
                System.out.println("    Result: Detected big-endian encoded vector counts (mismatch).");
                System.err.printf("  Issue: %s appears to be big-endian encoded (dimension %d, vectors %d).%n",
                    displayLabel,
                    result.getBigEndianDimension(),
                    result.getBigEndianVectorCount());
                System.err.println("  Suggestion: Re-run the export or conversion tooling with little-endian output enabled.");
                return true;
            }

            if (!result.isLittleEndianValid()) {
                String reason = result.getLittleEndianFailureReason();
                System.out.println("    Result: Unable to confirm little-endian layout.");
                System.err.printf("  Issue: Could not validate little-endian header for %s.%n", displayLabel);
                if (reason != null) {
                    System.err.printf("  Details: %s%n", reason);
                }
                System.err.println("  Suggestion: Verify the file is complete and follows the xvec specification.");
                return true;
            }

            System.out.println("    Result: Little-endian layout validated.");

            System.out.printf("%s: dimension %d, vectors %d%n",
                displayLabel,
                result.getLittleEndianDimension(),
                result.getLittleEndianVectorCount());

            if (result.isBigEndianValid()) {
                System.out.printf("  Step 3: Big-endian reinterpretation would report dimension %d, vectors %d.%n",
                    result.getBigEndianDimension(),
                    result.getBigEndianVectorCount());
            } else if (result.getBigEndianFailureReason() != null) {
                logger.debug("Big-endian interpretation rejected for {}: {}", displayLabel,
                    result.getBigEndianFailureReason());
                System.out.println("  Step 3: Big-endian reinterpretation rejected (expected for valid little-endian files).");
            }

            System.out.println("  Step 4: Endianness check complete.\n");
            return false;
        } catch (IOException e) {
            System.err.printf("  Error: Failed to inspect %s: %s%n", displayLabel, e.getMessage());
            System.err.println("  Suggestion: Ensure the file is readable and in a supported format.");
            return true;
        }
    }

    private Path expandPath(Path path) {
        if (path == null) {
            return null;
        }
        String expanded = path.toString()
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home"));
        return Path.of(expanded);
    }
}
