package io.nosqlbench.command.catalog;

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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nosqlbench.command.json.subcommands.export_json.Hdf5JsonSummarizer;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Alternative implementation of the catalog command, building hierarchical catalogs
 * in a simpler, declarative pass over all entries.
 */
@CommandLine.Command(name = "catalog",
    header = "Create catalog views of HDF5 files and dataset directories",
    description = "When given files or directories, recursively find dataset roots (directories with dataset.yaml)" +
                  " and .hdf5 files, and produce catalog.json and catalog.yaml files at each directory level.",
    exitCodeList = {"0: success", "1: error processing files"})
public class CMD_catalog implements Callable<Integer>, BundledCommand {

    private static final Logger logger = LogManager.getLogger(CMD_catalog.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Dump yamlDumper = new Dump(DumpSettings.builder().build());
    private static final Hdf5JsonSummarizer summarizer = new Hdf5JsonSummarizer();

    @CommandLine.Parameters(description = "Files and/or directories to catalog; Directories will be traversed to find dataset.yaml and .hdf5 files", arity = "0..*")
    private List<Path> inputs;

    @CommandLine.Option(names = "--basename", description = "Base name for catalog files (no extension)", defaultValue = "catalog")
    private String basename;

    ///  reate the CMD_catalog command
    public CMD_catalog() {}

    /// run a CMD_catalog command
    /// @param args command line arguments
    public static void main(String[] args) {
        CMD_catalog cmd = new CMD_catalog();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            // Default to current dir if no inputs
            if (inputs == null || inputs.isEmpty()) {
                inputs = List.of(Path.of("."));
            }
            // Validate basename
            if (basename.contains(".")) {
                logger.error("Basename must not contain a dot");
                return 1;
            }
            // Normalize inputs and validate existence
            List<Path> roots = new ArrayList<>();
            for (Path p : inputs) {
                Path abs = p.toAbsolutePath().normalize();
                if (!Files.exists(abs)) {
                    logger.error("Path does not exist: {}", abs);
                    return 1;
                }
                roots.add(abs);
            }
            // Determine common parent directory
            Path commonParent = findCommonParent(roots);
            // Gather all entries (dataset roots and hdf5 files)
            List<Entry> entries = new ArrayList<>();
            for (Path root : roots) {
                if (Files.isRegularFile(root) && root.toString().endsWith(".hdf5")) {
                    entries.add(loadHdf5Entry(root));
                } else if (Files.isDirectory(root)) {
                    Files.walkFileTree(root, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                        new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                Path yaml = dir.resolve("dataset.yaml");
                                if (Files.exists(yaml)) {
                                    // load dataset layout and all referenced files
                                    entries.addAll(loadDatasetEntries(yaml));
                                    return FileVisitResult.SKIP_SUBTREE;
                                }
                                return FileVisitResult.CONTINUE;
                            }
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (file.toString().endsWith(".hdf5")) {
                                    entries.add(loadHdf5Entry(file));
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        }
                    );
                }
            }
            // Determine all catalog directories (hierarchical)
            Set<Path> catalogDirs = new HashSet<>();
            for (Entry e : entries) {
                Path dir = e.path.getParent();
                while (dir != null && dir.startsWith(commonParent)) {
                    catalogDirs.add(dir);
                    dir = dir.getParent();
                }
            }
            // also ensure commonParent itself has a catalog
            catalogDirs.add(commonParent);
            // Write catalogs
            for (Path dir : catalogDirs) {
                List<Map<String, Object>> slice = entries.stream()
                    .filter(e -> e.path.startsWith(dir))
                    .map(e -> e.relativized(dir))
                    .collect(Collectors.toList());
                // sort by path for consistency
                slice.sort((a, b) -> a.getOrDefault("path", "").toString().compareTo(
                                b.getOrDefault("path", "").toString()));
                // write JSON
                Path jsonPath = dir.resolve(basename + ".json");
                Files.createDirectories(jsonPath.getParent());
                Files.writeString(jsonPath, gson.toJson(slice));
                // write YAML
                Path yamlPath = dir.resolve(basename + ".yaml");
                Files.writeString(yamlPath, yamlDumper.dumpToString(slice));
                logger.info("Wrote catalog {} entries to {}", slice.size(), jsonPath);
            }
            return 0;
        } catch (IOException e) {
            logger.error("Error creating catalog: {}", e.getMessage(), e);
            return 1;
        }
    }

    private Path findCommonParent(List<Path> paths) {
        Path common = paths.get(0);
        if (Files.isRegularFile(common)) common = common.getParent();
        common = common.toAbsolutePath().normalize();
        for (int i = 1; i < paths.size(); i++) {
            Path p = paths.get(i);
            if (Files.isRegularFile(p)) p = p.getParent();
            p = p.toAbsolutePath().normalize();
            while (!p.startsWith(common)) {
                common = common.getParent();
                if (common == null) break;
            }
        }
        return common != null ? common : Path.of(".");
    }

    private Entry loadDatasetEntry(Path yamlPath) {
        Map<String, Object> data = new HashMap<>();
        var layout = TestGroupLayout.load(yamlPath);
        data.put("layout", layout.toData());
        // Set entry name and type for dataset.yaml
        Path dir = yamlPath.getParent();
        String dsName = dir.getFileName().toString();
        data.put("name", dsName);
        data.put("dataset_type", "dataset.yaml");
        return new Entry(yamlPath.toAbsolutePath().normalize(), data);
    }

    private Entry loadHdf5Entry(Path file) {
        Map<String, Object> data;
        try {
            data = summarizer.describeFile(file);
        } catch (Exception e) {
            data = new HashMap<>();
            data.put("error", e.getMessage());
        }
        // Set entry name and type for .hdf5 files
        String fname = file.getFileName().toString();
        String base = fname.endsWith(".hdf5") ? fname.substring(0, fname.length() - 5) : fname;
        data.put("name", base);
        data.put("dataset_type", "hdf5");
        return new Entry(file.toAbsolutePath().normalize(), data);
    }

    /**
     * Loads the dataset entry and all files referenced under its "profiles" section.
     */
    @SuppressWarnings("unchecked")
    private List<Entry> loadDatasetEntries(Path yamlPath) {
        List<Entry> list = new ArrayList<>();
        // dataset.yaml entry
        Entry dsEntry = loadDatasetEntry(yamlPath);
        list.add(dsEntry);
        // extract profile sources
        Object layoutObj = dsEntry.data.get("layout");
        if (layoutObj instanceof Map<?, ?>) {
            Map<String, Object> layoutData = (Map<String, Object>) layoutObj;
            Object profilesObj = layoutData.get("profiles");
            if (profilesObj instanceof Map<?, ?>) {
                Map<String, Object> profiles = (Map<String, Object>) profilesObj;
        for (Object profVal : profiles.values()) {
            if (profVal instanceof Map<?, ?>) {
                Map<String, Object> profMap = (Map<String, Object>) profVal;
                for (Object entryVal : profMap.values()) {
                    if (entryVal instanceof Map<?, ?>) {
                        Map<String, Object> fileSpec = (Map<String, Object>) entryVal;
                        Object src = fileSpec.get("source");
                        if (src instanceof String) {
                            String srcStr = (String) src;
                            if (srcStr.endsWith(".hdf5")) {
                                Path fp = yamlPath.getParent().resolve(srcStr);
                                if (Files.exists(fp)) {
                                    list.add(loadHdf5Entry(fp));
                                }
                            }
                        }
                    }
                }
            }
        }
            }
        }
        return list;
    }


    /**
     * Helper to hold an entry's absolute path and its data map.
     */
    private static class Entry {
        final Path path;
        final Map<String, Object> data;
        Entry(Path path, Map<String, Object> data) {
            this.path = path;
            this.data = new HashMap<>(data);
        }
        Map<String, Object> relativized(Path baseDir) {
            Map<String, Object> m = new HashMap<>(data);
            Path rel = baseDir.relativize(path);
            m.put("path", rel.toString());
            return m;
        }
    }
}
