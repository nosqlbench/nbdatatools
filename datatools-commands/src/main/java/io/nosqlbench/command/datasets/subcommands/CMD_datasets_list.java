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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSView;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/// List datasets from accessible catalogs.
///
/// This command lists available vector testing datasets from configured or specified catalogs.
/// Output can be formatted as text, CSV, JSON, or YAML.
@CommandLine.Command(name = "list",
    aliases = {"ls"},
    header = "List datasets from accessible catalogs",
    description = "Browse and list vector testing datasets from accessible catalogs",
    exitCodeList = {"0: success", "1: error"})
public class CMD_datasets_list implements Callable<Integer> {

    /// Creates a new CMD_datasets_list instance.
    public CMD_datasets_list() {
    }

    private static final Logger logger = LogManager.getLogger(CMD_datasets_list.class);

    /// Output format options for listing datasets.
    public enum OutputFormat {
        /// Plain text output
        text,
        /// Comma-separated values output
        csv,
        /// JSON output
        json,
        /// YAML output
        yaml
    }

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--at"},
        description = "One or more catalog URLs or paths to use instead of configured catalogs")
    private List<String> atCatalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--verbose", "-v"}, description = "Show detailed information including URL, attributes, tags, and views")
    private boolean verbose = false;

    @CommandLine.Option(names = {"--format", "-f"},
        description = "Output format: ${COMPLETION-CANDIDATES}",
        defaultValue = "text")
    private OutputFormat format = OutputFormat.text;

    @Override
    public Integer call() {
        this.configdir =
            Path.of(this.configdir.toString().replace("~", System.getProperty("user.home"))
                .replace("${HOME}", System.getProperty("user.home")));

        TestDataSources config = new TestDataSources();

        if (!this.atCatalogs.isEmpty()) {
            config = config.addCatalogs(this.atCatalogs);
        } else {
            config = config.configure(this.configdir);
            config = config.addCatalogs(this.catalogs);
        }

        Catalog catalog = Catalog.of(config);

        switch (format) {
            case text -> outputText(catalog);
            case csv -> outputCsv(catalog);
            case json -> outputJson(catalog);
            case yaml -> outputYaml(catalog);
        }

        return 0;
    }

    private void outputText(Catalog catalog) {
        for (DatasetEntry entry : catalog.datasets()) {
            for (Map.Entry<String, DSProfile> profileEntry : entry.profiles().entrySet()) {
                String profileName = profileEntry.getKey();
                DSProfile profile = profileEntry.getValue();

                System.out.println(entry.name() + ":" + profileName);

                if (verbose) {
                    System.out.println(" url: " + (entry.url() != null ? entry.url().toString() : ""));

                    if (entry.attributes() != null && !entry.attributes().isEmpty()) {
                        for (Map.Entry<String, String> attr : entry.attributes().entrySet()) {
                            System.out.println(" " + attr.getKey() + ": " + attr.getValue());
                        }
                    }

                    if (entry.tags() != null && !entry.tags().isEmpty()) {
                        for (Map.Entry<String, String> tag : entry.tags().entrySet()) {
                            System.out.println(" tag." + tag.getKey() + ": " + tag.getValue());
                        }
                    }

                    if (profile.getMaxk() != null) {
                        System.out.println(" maxk: " + profile.getMaxk());
                    }

                    for (Map.Entry<String, DSView> viewEntry : profile.entrySet()) {
                        String viewName = viewEntry.getKey();
                        DSView view = viewEntry.getValue();
                        String sourcePath = view.getSource() != null && view.getSource().getPath() != null
                            ? view.getSource().getPath() : "";
                        System.out.println(" view." + viewName + ": " + sourcePath);
                    }
                }
            }
        }
    }

    private void outputCsv(Catalog catalog) {
        if (verbose) {
            System.out.println("dataset,profile,url,distance_function,model,vendor,license,maxk,views");
        } else {
            System.out.println("dataset,profile");
        }

        for (DatasetEntry entry : catalog.datasets()) {
            for (Map.Entry<String, DSProfile> profileEntry : entry.profiles().entrySet()) {
                String profileName = profileEntry.getKey();
                DSProfile profile = profileEntry.getValue();

                if (verbose) {
                    String url = entry.url() != null ? escapeCsv(entry.url().toString()) : "";
                    String distanceFunction = getAttr(entry, "distance_function");
                    String model = getAttr(entry, "model");
                    String vendor = getAttr(entry, "vendor");
                    String license = getAttr(entry, "license");
                    String maxk = profile.getMaxk() != null ? profile.getMaxk().toString() : "";
                    String views = profile.keySet().stream().collect(Collectors.joining(";"));

                    System.out.println(String.join(",",
                        escapeCsv(entry.name()),
                        escapeCsv(profileName),
                        url,
                        escapeCsv(distanceFunction),
                        escapeCsv(model),
                        escapeCsv(vendor),
                        escapeCsv(license),
                        maxk,
                        escapeCsv(views)
                    ));
                } else {
                    System.out.println(escapeCsv(entry.name()) + "," + escapeCsv(profileName));
                }
            }
        }
    }

    private void outputJson(Catalog catalog) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        List<Map<String, Object>> output = buildStructuredOutput(catalog);
        System.out.println(gson.toJson(output));
    }

    private void outputYaml(Catalog catalog) {
        List<Map<String, Object>> output = buildStructuredOutput(catalog);
        for (Map<String, Object> item : output) {
            printYaml(item, 0);
            System.out.println();
        }
    }

    private List<Map<String, Object>> buildStructuredOutput(Catalog catalog) {
        List<Map<String, Object>> output = new ArrayList<>();

        for (DatasetEntry entry : catalog.datasets()) {
            for (Map.Entry<String, DSProfile> profileEntry : entry.profiles().entrySet()) {
                String profileName = profileEntry.getKey();
                DSProfile profile = profileEntry.getValue();

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("dataset", entry.name());
                item.put("profile", profileName);

                if (verbose) {
                    item.put("url", entry.url() != null ? entry.url().toString() : null);

                    if (entry.attributes() != null && !entry.attributes().isEmpty()) {
                        item.put("attributes", new LinkedHashMap<>(entry.attributes()));
                    }

                    if (entry.tags() != null && !entry.tags().isEmpty()) {
                        item.put("tags", new LinkedHashMap<>(entry.tags()));
                    }

                    if (profile.getMaxk() != null) {
                        item.put("maxk", profile.getMaxk());
                    }

                    Map<String, String> views = new LinkedHashMap<>();
                    for (Map.Entry<String, DSView> viewEntry : profile.entrySet()) {
                        String viewName = viewEntry.getKey();
                        DSView view = viewEntry.getValue();
                        String sourcePath = view.getSource() != null && view.getSource().getPath() != null
                            ? view.getSource().getPath() : "";
                        views.put(viewName, sourcePath);
                    }
                    if (!views.isEmpty()) {
                        item.put("views", views);
                    }
                }

                output.add(item);
            }
        }

        return output;
    }

    private void printYaml(Map<String, Object> map, int indent) {
        String prefix = "  ".repeat(indent);
        boolean first = true;

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (first && indent == 0) {
                System.out.print("- ");
                first = false;
            } else if (indent == 0) {
                System.out.print("  ");
            } else {
                System.out.print(prefix);
            }

            if (value == null) {
                System.out.println(key + ":");
            } else if (value instanceof Map) {
                System.out.println(key + ":");
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) value;
                for (Map.Entry<String, Object> nestedEntry : nested.entrySet()) {
                    System.out.print(prefix + "  ");
                    System.out.println(nestedEntry.getKey() + ": " + formatYamlValue(nestedEntry.getValue()));
                }
            } else {
                System.out.println(key + ": " + formatYamlValue(value));
            }
        }
    }

    private String formatYamlValue(Object value) {
        if (value == null) {
            return "";
        }
        String str = value.toString();
        if (str.contains(":") || str.contains("#") || str.contains("'") || str.contains("\"")
            || str.startsWith(" ") || str.endsWith(" "))
        {
            return "\"" + str.replace("\"", "\\\"") + "\"";
        }
        return str;
    }

    private String getAttr(DatasetEntry entry, String key) {
        if (entry.attributes() == null) {
            return "";
        }
        return entry.attributes().getOrDefault(key, "");
    }

    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
