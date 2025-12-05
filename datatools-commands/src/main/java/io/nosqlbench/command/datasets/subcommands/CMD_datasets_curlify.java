package io.nosqlbench.command.datasets.subcommands;

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


import io.nosqlbench.command.common.CommandLineFormatter;
import io.nosqlbench.vectordata.layout.FGroup;
import io.nosqlbench.vectordata.layout.FInterval;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.layout.FView;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/// Emit curl commands to download a remote dataset.yaml and only the needed byte-ranges
/// for the selected profiles.
@CommandLine.Command(name = "curlify",
    header = "Generate curl commands for a remote dataset.yaml",
    description = "Downloads the dataset.yaml and emits curl commands which use HTTP range reads to\n" +
        "fetch only the portions of each referenced file that are needed for the selected profiles.\n" +
        "If no profiles are specified, all profiles are included.")
public class CMD_datasets_curlify implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_datasets_curlify.class);
    private static final Pattern REMOTE_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.+");

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Parameters(paramLabel = "DATASET_YAML_URL",
        description = "Remote dataset.yaml URL",
        arity = "1")
    private String datasetYamlUrl;

    @CommandLine.Option(names = {"-p", "--profile"},
        description = "Profiles to include (default: all). Comma-separated list allowed.",
        split = ",")
    private List<String> profileNames = new ArrayList<>();

    @CommandLine.Option(names = {"-o", "--output"},
        description = "Output directory (default: dataset name)")
    private Path outputDir;

    @Override
    public Integer call() {
        CommandLineFormatter.printCommandLine(spec);
        spec.commandLine().getOut().println();

        StringBuilder sb = new StringBuilder();
        List<String> errors = new ArrayList<>();
        int exitCode = 0;

        URI yamlUri = null;
        String datasetName = "dataset";
        Path targetDir = outputDir != null ? outputDir : Path.of(datasetName);

        try {
            try {
                yamlUri = URI.create(datasetYamlUrl);
                datasetName = deriveDatasetName(yamlUri);
                targetDir = outputDir != null ? outputDir : Path.of(datasetName);
            } catch (Exception e) {
                String msg = "Invalid dataset.yaml URI: " + e.getMessage();
                errors.add(msg);
                logger.debug("invalid uri", e);
            }

        sb.append("# curl script for dataset '").append(datasetName).append("'\n");
        sb.append("# source: ").append(datasetYamlUrl).append("\n");
        sb.append("DATASET_DIR=").append(shQuote(targetDir.toString())).append("\n");
        sb.append("mkdir -p \"$DATASET_DIR\"\n");
        if (yamlUri != null) {
            sb.append("curl -L -o \"$DATASET_DIR/dataset.yaml\" ").append(shQuote(yamlUri.toString())).append("\n\n");
        } else {
            sb.append("# Unable to curl dataset.yaml because the URL was invalid\n\n");
            exitCode = 1;
        }

        TestGroupLayout layout = null;
        if (yamlUri != null) {
            sb.append("# [info] Fetching dataset.yaml ...\n");
            String yamlContent = null;
            try {
                yamlContent = fetchText(yamlUri);
                sb.append("# [info] Fetched dataset.yaml\n");
            } catch (Exception e) {
                String msg = "Failed to fetch dataset.yaml: " + e.getMessage();
                errors.add(msg);
                sb.append("# ").append(msg).append("\n");
                logger.debug("fetch failure", e);
                exitCode = 1;
            }

            if (yamlContent != null) {
                try {
                    layout = TestGroupLayout.fromYaml(yamlContent);
                    sb.append("# [info] Parsed dataset.yaml\n");
                } catch (RuntimeException rte) {
                    String msg = String.format("Unable to parse dataset.yaml from %s: %s", yamlUri, rte.getMessage());
                    errors.add(msg);
                    sb.append("# ").append(msg).append("\n");
                    logger.debug("parse failure", rte);
                    exitCode = 1;
                }
            }
        }

        Path cacheRoot = null;
        if (layout != null) {
            cacheRoot = targetDir.resolve(".curlify-cache");
            try {
                Files.createDirectories(cacheRoot);
                sb.append("# [info] Cache dir: ").append(cacheRoot).append("\n");
            } catch (IOException e) {
                String msg = String.format("Unable to create cache directory %s: %s", cacheRoot, e.getMessage());
                errors.add(msg);
                sb.append("# ").append(msg).append("\n");
                layout = null;
                exitCode = 1;
            }
        }

        if (layout != null) {
            sb.append("# [info] Mapping profiles...\n");
            FGroup profilesGroup = layout.profiles();
            Map<String, FProfiles> profiles = profilesGroup.profiles();
            Set<String> requestedProfiles = profileNames.isEmpty()
                ? null
                : new java.util.LinkedHashSet<>(profileNames);
            List<String> datasetProfileOrder = new ArrayList<>(profiles.keySet());

            if (requestedProfiles != null) {
                for (String pname : requestedProfiles) {
                    if (!profiles.containsKey(pname)) {
                        String msg = String.format("Profile '%s' not found in dataset.yaml", pname);
                        errors.add(msg);
                        sb.append("# ").append(msg).append("\n");
                        exitCode = 1;
                    }
                }
            }

            List<String> orderedProfiles = new ArrayList<>();
            for (String pname : datasetProfileOrder) {
                if (requestedProfiles == null || requestedProfiles.contains(pname)) {
                    orderedProfiles.add(pname);
                }
            }

            URI baseUri = yamlUri.resolve(".");

            for (String profileName : orderedProfiles) {
                FProfiles fProfile = profiles.get(profileName);
                if (fProfile == null) {
                    String msg = String.format("Profile '%s' not found in dataset.yaml", profileName);
                    errors.add(msg);
                    sb.append("# ").append(msg).append("\n");
                    exitCode = 1;
                    continue;
                }

                sb.append("# Profile: ").append(profileName).append("\n");
                for (Map.Entry<String, FView> vEntry : fProfile.views().entrySet()) {
                    String viewName = vEntry.getKey();
                    FView view = vEntry.getValue();
                    URI resolved = resolveSource(baseUri, view.source().inpath());
                    String localPath = localPathFor(view.source().inpath(), resolved, targetDir);
                    Path localPathObj = Path.of(localPath);
                    String parentDir = localPathObj.getParent() != null
                        ? localPathObj.getParent().toString()
                        : ".";

                    sb.append("# ").append(viewName);
                    if (!isAllWindow(view.window())) {
                        sb.append(" window: ").append(view.window().toData());
                    }
                    sb.append("\n");
                    sb.append("mkdir -p ").append(shQuote(parentDir)).append("\n");

                    if (isAllWindow(view.window())) {
                        sb.append("curl -L -C - -o ").append(shQuote(localPath))
                            .append(" ").append(shQuote(resolved.toString())).append("\n\n");
                    } else {
                        int componentBytes = componentBytes(extensionOf(resolved.getPath()));
                        try {
                            ViewSizing sizing = determineSizing(viewName, view, resolved, fProfile.maxk(), componentBytes, cacheRoot);
                            emitWindowedDownload(sb, viewName, view, resolved, localPath, sizing);
                        } catch (Exception e) {
                            String msg = String.format("Failed to size view '%s' in profile '%s': %s", viewName, profileName, e.getMessage());
                            errors.add(msg);
                            sb.append("# ").append(msg).append("\n\n");
                            logger.debug("sizing failure", e);
                            exitCode = 1;
                        }
                    }
                }
                sb.append("\n");
            }
        }
        } catch (Exception e) {
            String msg = "Failed to curlify dataset: " + e.getMessage();
            logger.debug("curlify failed", e);
            errors.add(msg);
            sb.append("# ").append(msg).append("\n");
            exitCode = 1;
        } finally {
            if (!errors.isEmpty()) {
                sb.append("# Errors encountered during dataset mapping:\n");
                for (String msg : errors) {
                    sb.append("# ").append(msg).append("\n");
                }
            }
            spec.commandLine().getOut().print(sb.toString());
            spec.commandLine().getOut().flush();
            if (!errors.isEmpty()) {
                errors.forEach(spec.commandLine().getErr()::println);
            }
        }
        return exitCode;
    }

    private void emitWindowedDownload(StringBuilder sb, String viewName, FView view, URI sourceUri, String localPath, ViewSizing sizing) {
        FWindow window = view.window();

        sb.append("# ").append(viewName)
            .append(" dim=").append(sizing.dimension())
            .append(" record_size=").append(sizing.recordSize())
            .append("\n");
        List<String> parts = new ArrayList<>();
        int idx = 0;
        for (FInterval interval : window.intervals()) {
            long start = interval.minIncl();
            long endExcl = interval.maxExcl();
            long startByte = start * sizing.recordSize();
            long endByte = (endExcl * sizing.recordSize()) - 1;

            if (window.intervals().size() == 1) {
                sb.append("# bytes ").append(startByte).append("-").append(endByte).append(" (records ").append(start).append("..").append(endExcl - 1).append(")\n");
                sb.append("curl -L -C - --range ").append(startByte).append("-").append(endByte)
                    .append(" -o ").append(shQuote(localPath)).append(" ")
                    .append(shQuote(sourceUri.toString())).append("\n");
                return;
            }

            String partPath = localPath + ".part" + idx;
            parts.add(partPath);
            sb.append("# part ").append(idx).append(" bytes ").append(startByte).append("-").append(endByte)
                .append(" (records ").append(start).append("..").append(endExcl - 1).append(")\n");
            sb.append("curl -L -C - --range ").append(startByte).append("-").append(endByte)
                .append(" -o ").append(shQuote(partPath)).append(" ")
                .append(shQuote(sourceUri.toString())).append("\n");
            idx++;
        }
        if (!parts.isEmpty()) {
            sb.append("cat");
            for (String part : parts) {
                sb.append(" ").append(shQuote(part));
            }
            sb.append(" > ").append(shQuote(localPath)).append("\n");
            sb.append("rm -f");
            for (String part : parts) {
                sb.append(" ").append(shQuote(part));
            }
            sb.append("\n\n");
        }
    }

    private static boolean isAllWindow(FWindow window) {
        return window == null
            || window.intervals().size() == 1
            && window.intervals().get(0).minIncl() == -1
            && window.intervals().get(0).maxExcl() == -1;
    }

    private URI resolveSource(URI base, String sourceSpec) {
        if (REMOTE_PATTERN.matcher(sourceSpec).matches()) {
            return URI.create(sourceSpec);
        }
        return base.resolve(sourceSpec);
    }

    private String localPathFor(String sourceSpec, URI resolved, Path targetDir) {
        String relative = sourceSpec;
        if (REMOTE_PATTERN.matcher(sourceSpec).matches()) {
            String path = resolved.getPath();
            if (path == null || path.isEmpty() || "/".equals(path)) {
                path = "remote.bin";
            }
            relative = path.startsWith("/") ? path.substring(1) : path;
        }
        if (relative.startsWith("./")) {
            relative = relative.substring(2);
        }
        return targetDir.resolve(relative).normalize().toString();
    }

    private String deriveDatasetName(URI yamlUri) {
        String path = yamlUri.getPath();
        if (path == null || path.isEmpty()) {
            return "dataset";
        }
        Path p = Path.of(path);
        if ("dataset.yaml".equalsIgnoreCase(p.getFileName().toString()) && p.getParent() != null) {
            return p.getParent().getFileName().toString();
        }
        String filename = p.getFileName().toString();
        int dot = filename.lastIndexOf('.');
        return dot > 0 ? filename.substring(0, dot) : filename;
    }

    private String fetchText(URI uri) throws IOException, InterruptedException {
        if ("file".equalsIgnoreCase(uri.getScheme()) || uri.getScheme() == null) {
            Path p = uri.getScheme() == null ? Path.of(uri.toString()) : Path.of(uri);
            return Files.readString(p);
        }
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode());
        }
        return response.body();
    }

    private String extensionOf(String path) {
        int dot = path.lastIndexOf('.');
        if (dot < 0 || dot == path.length() - 1) {
            return "";
        }
        return path.substring(dot + 1).toLowerCase();
    }

    private int componentBytes(String ext) {
        switch (ext) {
            case "ivec":
            case "ivecs":
                return 4;
            case "bvec":
            case "bvecs":
                return 1;
            case "fvec":
            case "fvecs":
            default:
                return 4;
        }
    }

    private String sanitize(String input) {
        return input.replaceAll("[^A-Za-z0-9]", "_");
    }

    private String shQuote(String value) {
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

    private ViewSizing determineSizing(String viewName, FView view, URI sourceUri, Integer profileMaxK, int componentBytes, Path cacheRoot) throws Exception {
        int dimension;
        if (profileMaxK != null && ("neighbor_indices".equals(viewName) || "neighbor_distances".equals(viewName))) {
            dimension = profileMaxK;
        } else {
            dimension = fetchDimension(sourceUri, cacheRoot);
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension for " + viewName + " was " + dimension);
        }
        int recordSize = 4 + (dimension * componentBytes);
        return new ViewSizing(dimension, recordSize);
    }

    private int fetchDimension(URI sourceUri, Path cacheRoot) throws Exception {
        try (AsynchronousFileChannel channel = openChannel(sourceUri, cacheRoot)) {
            ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            Future<Integer> read = channel.read(header, 0);
            int readBytes = read.get();
            if (readBytes < 4) {
                throw new IOException("Expected 4 bytes but got " + readBytes + " from " + sourceUri);
            }
            header.flip();
            return header.getInt();
        }
    }

    private AsynchronousFileChannel openChannel(URI uri, Path cacheRoot) throws IOException {
        if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
            String filename = Path.of(uri.getPath()).getFileName() != null
                ? Path.of(uri.getPath()).getFileName().toString()
                : "remote.bin";
            Path cacheFile = cacheRoot.resolve(filename);
            Files.createDirectories(cacheFile.getParent());
            // Use vectordata merkle-aware channel for ranged reads and caching
            return new io.nosqlbench.vectordata.merklev2.MAFileChannel(
                cacheFile,
                cacheFile.resolveSibling(cacheFile.getFileName() + ".mrkl"),
                uri.toString(),
                64 * 1024 // only require header-sized cache space when sizing
            );
        }
        Path p = "file".equalsIgnoreCase(uri.getScheme()) || uri.getScheme() == null
            ? Path.of(uri.getPath())
            : Path.of(uri);
        return AsynchronousFileChannel.open(p, StandardOpenOption.READ);
    }

    private record ViewSizing(int dimension, int recordSize) {}
}
