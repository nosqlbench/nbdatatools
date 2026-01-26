/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.nosqlbench.command.datasets.subcommands;

import io.nosqlbench.vectordata.config.VectorDataSettings;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/// List locally cached datasets, profiles, and facets present in the cache directory.
@CommandLine.Command(name = "cache",
    header = "List locally cached datasets and profiles",
    description = "Shows which dataset profiles and facets are cached in the local cache directory",
    exitCodeList = {"0: success", "1: error"})
public class CMD_datasets_cache implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_datasets_cache.class);

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--cache-dir"},
        description = "Directory for cached dataset files (defaults to configured cache_dir)")
    private Path cacheDir;

    @Override
    public Integer call() {
        try {
            this.configdir = expandPath(this.configdir);
            this.cacheDir = resolveCacheDir();
            if (this.cacheDir == null) {
                return 1;
            }

            if (!Files.exists(cacheDir) || !Files.isDirectory(cacheDir)) {
                System.out.println("Cache directory not found: " + cacheDir);
                return 0;
            }

            TestDataSources config = new TestDataSources().configure(this.configdir);
            if (this.catalogs != null && !this.catalogs.isEmpty()) {
                config = config.addCatalogs(this.catalogs);
            }

            Catalog catalog = Catalog.of(config);
            Map<String, Map<String, List<String>>> cached = new LinkedHashMap<>();

            for (DatasetEntry entry : catalog.datasets()) {
                String datasetName = entry.name();
                Path datasetDir = cacheDir.resolve(datasetName);
                if (!Files.isDirectory(datasetDir) || entry.profiles() == null) {
                    continue;
                }

                Map<String, List<String>> profiles = new LinkedHashMap<>();
                for (Map.Entry<String, DSProfile> profileEntry : entry.profiles().entrySet()) {
                    String profileName = profileEntry.getKey();
                    DSProfile profile = profileEntry.getValue();
                    if (profile == null) {
                        continue;
                    }

                    List<String> facets = cachedFacets(datasetDir, profileName, profile);
                    if (!facets.isEmpty()) {
                        profiles.put(profileName, facets);
                    }
                }

                if (!profiles.isEmpty()) {
                    cached.put(datasetName, profiles);
                }
            }

            if (cached.isEmpty()) {
                System.out.println("No cached dataset profiles found in " + cacheDir);
                return 0;
            }

            System.out.println("Cache directory: " + cacheDir);
            for (Map.Entry<String, Map<String, List<String>>> datasetEntry : cached.entrySet()) {
                String datasetName = datasetEntry.getKey();
                for (Map.Entry<String, List<String>> profileEntry : datasetEntry.getValue().entrySet()) {
                    String profileName = profileEntry.getKey();
                    List<String> facets = profileEntry.getValue();
                    System.out.println(datasetName + "." + profileName + " -> " + String.join(", ", facets));
                }
            }
            return 0;

        } catch (Exception e) {
            System.err.println("Error listing cached datasets: " + e.getMessage());
            logger.debug("cache listing failed", e);
            return 1;
        }
    }

    private List<String> cachedFacets(Path datasetDir, String profileName, DSProfile profile) {
        DSView baseView = findBaseVectorsView(profile);
        if (baseView == null) {
            return List.of();
        }
        if (!isBaseVectorsComplete(datasetDir, profileName, baseView)) {
            return List.of();
        }
        LinkedHashSet<String> facets = new LinkedHashSet<>();
        for (Map.Entry<String, DSView> viewEntry : profile.entrySet()) {
            DSView view = viewEntry.getValue();
            if (view == null || view.getSource() == null || view.getSource().getPath() == null) {
                continue;
            }
            Path sharedPath = datasetDir.resolve(view.getSource().getPath());
            Path legacyPath = datasetDir.resolve(profileName).resolve(view.getSource().getPath());
            if (existsEither(sharedPath) || existsEither(legacyPath)) {
                String viewName = viewEntry.getKey();
                Optional<TestDataKind> kind = TestDataKind.fromOptionalString(viewName);
                if (kind.isEmpty() && view.getName() != null) {
                    kind = TestDataKind.fromOptionalString(view.getName());
                }
                String canonicalName = kind.map(TestDataKind::name).orElse(viewName);
                if (TestDataKind.base_vectors.name().equals(canonicalName)) {
                    facets.add(canonicalName);
                } else {
                    facets.add(canonicalName);
                }
            }
        }
        return new ArrayList<>(facets);
    }

    private DSView findBaseVectorsView(DSProfile profile) {
        for (Map.Entry<String, DSView> entry : profile.entrySet()) {
            DSView view = entry.getValue();
            if (view == null) {
                continue;
            }
            Optional<TestDataKind> kind = TestDataKind.fromOptionalString(entry.getKey());
            if (kind.isEmpty() && view.getName() != null) {
                kind = TestDataKind.fromOptionalString(view.getName());
            }
            if (kind.isPresent() && kind.get() == TestDataKind.base_vectors) {
                return view;
            }
        }
        return null;
    }

    private boolean isBaseVectorsComplete(Path datasetDir, String profileName, DSView baseView) {
        if (baseView == null || baseView.getSource() == null || baseView.getSource().getPath() == null) {
            return false;
        }

        Path sharedPath = datasetDir.resolve(baseView.getSource().getPath());
        Path legacyPath = datasetDir.resolve(profileName).resolve(baseView.getSource().getPath());
        Path contentPath = existsEither(sharedPath) ? sharedPath : legacyPath;
        if (!existsEither(contentPath)) {
            return false;
        }

        Path merklePath = contentPath.resolveSibling(contentPath.getFileName() + ".mrkl");
        if (!Files.exists(merklePath)) {
            return false;
        }

        long recordSize = recordSizeFor(contentPath, baseView);
        if (recordSize <= 0) {
            return false;
        }

        try (MerkleState state = MerkleState.load(merklePath)) {
            MerkleShape shape = state.getMerkleShape();
            long requiredBytes = requiredBytesForWindow(baseView.getWindow(), recordSize, shape);
            if (requiredBytes < 0) {
                return false;
            }
            long contiguousBytes = contiguousValidBytes(state, shape);
            return contiguousBytes >= requiredBytes;
        } catch (IOException e) {
            logger.debug("Failed to read merkle state: {}", e.getMessage());
            return false;
        }
    }

    private long requiredBytesForWindow(DSWindow window, long recordSize, MerkleShape shape) {
        if (window == null || window.isEmpty()) {
            return shape.getTotalContentSize();
        }
        long maxEnd = 0;
        for (var interval : window) {
            maxEnd = Math.max(maxEnd, interval.getMaxExcl());
        }
        return maxEnd * recordSize;
    }

    private long contiguousValidBytes(MerkleState state, MerkleShape shape) {
        BitSet valid = state.getValidChunks();
        int totalChunks = shape.getTotalChunks();
        int firstInvalid = valid.nextClearBit(0);
        int contiguousChunks = Math.min(firstInvalid, totalChunks);
        if (contiguousChunks <= 0) {
            return 0L;
        }
        if (contiguousChunks >= totalChunks) {
            return shape.getTotalContentSize();
        }
        return shape.getChunkStartPosition(contiguousChunks);
    }

    private long recordSizeFor(Path contentPath, DSView view) {
        String extension = fileExtension(contentPath.getFileName().toString());
        if (extension == null) {
            return -1;
        }
        int componentBytes = componentBytesFromExtension(extension);
        if (componentBytes <= 0) {
            return -1;
        }
        int dimensions = readDimensions(contentPath);
        if (dimensions <= 0) {
            return -1;
        }
        return 4L + (long) dimensions * componentBytes;
    }

    private String fileExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        if (dot < 0 || dot == fileName.length() - 1) {
            return null;
        }
        return fileName.substring(dot + 1).toLowerCase();
    }

    private int componentBytesFromExtension(String extension) {
        return switch (extension) {
            case "ivec", "ivecs" -> Integer.BYTES;
            case "bvec", "bvecs" -> Byte.BYTES;
            case "fvec", "fvecs" -> Float.BYTES;
            case "dvec", "dvecs" -> Double.BYTES;
            default -> -1;
        };
    }

    private int readDimensions(Path contentPath) {
        if (!Files.exists(contentPath)) {
            return -1;
        }
        try {
            if (Files.size(contentPath) < 4) {
                return -1;
            }
            ByteBuffer dimBuffer = ByteBuffer.allocate(4);
            dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
            try (var channel = Files.newByteChannel(contentPath)) {
                int read = channel.read(dimBuffer);
                if (read != 4) {
                    return -1;
                }
            }
            dimBuffer.flip();
            int dimensions = dimBuffer.getInt();
            return dimensions > 0 ? dimensions : -1;
        } catch (IOException e) {
            logger.debug("Failed to read dimensions from {}: {}", contentPath, e.getMessage());
            return -1;
        }
    }

    private boolean existsEither(Path contentPath) {
        if (Files.exists(contentPath)) {
            return true;
        }
        Path merklePath = contentPath.resolveSibling(contentPath.getFileName() + ".mrkl");
        return Files.exists(merklePath);
    }

    private Path expandPath(Path path) {
        return Paths.get(path.toString()
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home")));
    }

    private Path resolveCacheDir() {
        if (cacheDir != null) {
            return expandPath(cacheDir);
        }
        if (!VectorDataSettings.isConfigured()) {
            System.err.println("cache_dir is not configured. Run: nbvectors config init");
            return null;
        }
        return VectorDataSettings.load().getCacheDirectory();
    }
}
