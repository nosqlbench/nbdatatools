package io.nosqlbench.command.vectordata;

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

import io.nosqlbench.vectordata.discovery.DatasetLoader;
import io.nosqlbench.vectordata.discovery.FilesystemTestDataGroup;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import picocli.CommandLine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/// Utilities shared across vectordata CLI commands (one-shot and REPL).
public class VectordataCliSupport {

    public static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);

    /// Log the invocation at debug level without polluting stdout.
    public static void logInvocation(CommandLine.Model.CommandSpec spec, org.apache.logging.log4j.Logger logger) {
        try {
            var pr = spec.commandLine().getParseResult();
            if (pr != null) {
                logger.debug("vectordata invocation: {}", pr.originalArgs());
            }
        } catch (Exception ignored) {
            // best-effort only
        }
    }

    public record DatasetSession(String name, String source, ProfileSelector selector, List<String> profileNames) implements Closeable {
        @Override
        public void close() {
            try {
                selector.close();
            } catch (Exception ignored) {
            }
        }
    }

    public static DatasetSession loadDataset(String datasetSpec) throws Exception {
        return loadDataset(datasetSpec, null);
    }

    public static DatasetSession loadDataset(String datasetSpec, String cacheDir) throws Exception {
        String expanded = expandHome(datasetSpec);
        ProfileSelector selector;
        String name;
        String source;

        if (looksLikePathOrUrl(expanded)) {
            selector = DatasetLoader.load(expanded, cacheDir);
            name = deriveDatasetName(expanded, selector);
            source = expanded;
        } else {
            Catalog catalog = Catalog.of(new TestDataSources().configureOptional(Path.of(System.getProperty("user.home"), ".config", "vectordata")));
            Optional<DatasetEntry> match = catalog.findExact(datasetSpec);
            if (match.isEmpty()) {
                match = catalog.matchOne(datasetSpec);
            }
            if (match.isEmpty()) {
                throw new IllegalArgumentException("Dataset '" + datasetSpec + "' not found in catalog and does not look like a file/URL");
            }
            DatasetEntry entry = match.get();
            selector = entry.select();
            name = entry.name();
            source = entry.url() != null ? entry.url().toString() : datasetSpec;
        }

        List<String> profiles = orderedProfiles(selector);
        return new DatasetSession(name, source, selector, profiles);
    }

    private static String deriveDatasetName(String spec, ProfileSelector selector) {
        if (selector instanceof FilesystemTestDataGroup fs) {
            return fs.getName();
        }
        if (selector instanceof TestDataGroup tg) {
            return tg.getName();
        }
        Path asPath = Path.of(spec);
        if (Files.exists(asPath)) {
            if (asPath.getFileName() != null) {
                String filename = asPath.getFileName().toString();
                int dot = filename.lastIndexOf('.');
                return dot > 0 ? filename.substring(0, dot) : filename;
            }
        }
        // Fallback: last path segment of URI or spec
        int slash = spec.lastIndexOf('/');
        if (slash >= 0 && slash < spec.length() - 1) {
            String tail = spec.substring(slash + 1);
            int dot = tail.lastIndexOf('.');
            return dot > 0 ? tail.substring(0, dot) : tail;
        }
        return spec;
    }

    private static boolean looksLikePathOrUrl(String spec) {
        if (spec.contains("://")) {
            return true;
        }
        Path p = Path.of(spec);
        if (Files.exists(p)) {
            return true;
        }
        return spec.contains("/") || spec.contains("\\") || spec.endsWith(".yaml");
    }

    private static String expandHome(String path) {
        if (path == null) {
            return null;
        }
        String home = System.getProperty("user.home");
        return path.replace("~", home).replace("${HOME}", home);
    }

    private static List<String> orderedProfiles(ProfileSelector selector) {
        List<String> names = new ArrayList<>();
        if (selector instanceof io.nosqlbench.vectordata.downloader.VirtualProfileSelector vps) {
            names.addAll(vps.profiles());
        } else if (selector instanceof FilesystemTestDataGroup fs) {
            names.addAll(fs.getProfileNames());
        } else if (selector instanceof TestDataGroup tg) {
            names.addAll(tg.getProfileNames());
        }
        selector.presetProfile().ifPresent(p -> {
            if (!names.contains(p)) {
                names.add(0, p);
            }
        });
        if (names.isEmpty()) {
            names.add("default");
        }
        // Preserve order; no sorting
        return names;
    }

    public static void ensureDataset(CommandLine.Model.CommandSpec spec, DatasetSession handle) {
        if (handle == null) {
            throw new CommandLine.ParameterException(spec.commandLine(), "No dataset is loaded. Provide --dataset or use the repl 'use' command.");
        }
    }

    public static void writeJson(java.io.PrintWriter out, Object value) throws java.io.IOException {
        out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value));
        out.flush();
    }

    /// Very small arg tokenizer for the REPL; supports single and double quotes.
    public static String[] tokenize(String line) {
        List<String> parts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                continue;
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                continue;
            } else if (Character.isWhitespace(c) && !inSingle && !inDouble) {
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(c);
        }
        if (current.length() > 0) {
            parts.add(current.toString());
        }
        return parts.toArray(new String[0]);
    }
}
