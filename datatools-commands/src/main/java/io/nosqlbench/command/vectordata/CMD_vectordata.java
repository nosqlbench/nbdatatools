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

import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jline.reader.Candidate;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "vectordata",
    header = "Explore vectordata layouts and views",
    description = "Inspect dataset.yaml layouts and views via the vectordata API, with an optional REPL.",
    subcommands = {
        CMD_vectordata_info.class,
        CMD_vectordata_views.class,
        CMD_vectordata_profiles.class,
        CMD_vectordata_size.class,
        CMD_vectordata_sample.class,
        CMD_vectordata_prebuffer.class,
        CMD_vectordata_cat.class,
        CMD_vectordata_verify.class,
        CMD_vectordata_datasets.class,
        CMD_vectordata_repl.class,
        CommandLine.HelpCommand.class
    })
public class CMD_vectordata implements Callable<Integer>, BundledCommand {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata.class);

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    static String windowText(FWindow window) {
        if (window == null || (window.intervals().size() == 1
            && window.intervals().get(0).minIncl() == -1
            && window.intervals().get(0).maxExcl() == -1)) {
            return "ALL";
        }
        return window.toData();
    }
}

@CommandLine.Command(name = "datasets", description = "List datasets available from configured catalogs")
class CMD_vectordata_datasets implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_datasets.class);

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"--configdir"}, description = "Configuration directory containing catalogs.yaml", defaultValue = "~/.config/vectordata")
    private String configDir;

    @CommandLine.Option(names = {"--catalog"}, description = "Additional catalog directories or URLs to include", split = ",")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--at"}, description = "Catalog URLs or paths to use instead of configured catalogs", split = ",")
    private List<String> atCatalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @CommandLine.Option(names = {"--verbose", "-v"}, description = "Include URL, tags, and attributes in text output")
    private boolean verbose;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            TestDataSources sources = new TestDataSources();
            if (!atCatalogs.isEmpty()) {
                sources = sources.addCatalogs(atCatalogs.toArray(new String[0]));
            } else {
                String expandedConfig = configDir.replace("~", System.getProperty("user.home"))
                    .replace("${HOME}", System.getProperty("user.home"));
                sources = sources.configure(java.nio.file.Path.of(expandedConfig));
                if (!catalogs.isEmpty()) {
                    sources = sources.addCatalogs(catalogs.toArray(new String[0]));
                }
            }

            Catalog catalog = Catalog.of(sources);
            printDatasets(spec.commandLine().getOut(), catalog.datasets(), format, verbose);
            return 0;
        } catch (Exception e) {
            String msg = "Failed to list datasets: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("datasets failed", e);
            return 1;
        }
    }

    static void printDatasets(PrintWriter out, List<DatasetEntry> entries, String format, boolean verbose) throws java.io.IOException {
        if ("json".equalsIgnoreCase(format)) {
            List<Map<String, Object>> payload = new ArrayList<>();
            for (DatasetEntry entry : entries) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("name", entry.name());
                row.put("url", entry.url() != null ? entry.url().toString() : null);
                row.put("profiles", new ArrayList<>(entry.profiles().keySet()));
                if (entry.tags() != null) {
                    row.put("tags", entry.tags());
                }
                if (entry.attributes() != null) {
                    row.put("attributes", entry.attributes());
                }
                payload.add(row);
            }
            VectordataCliSupport.writeJson(out, payload);
        } else {
            out.printf("Datasets (%d)%n", entries.size());
            for (DatasetEntry entry : entries) {
                out.printf("- %s", entry.name());
                if (entry.url() != null) {
                    out.printf(" [%s]", entry.url());
                }
                out.printf("%n  profiles: %s%n", String.join(", ", entry.profiles().keySet()));
                if (verbose) {
                    if (entry.tags() != null && !entry.tags().isEmpty()) {
                        out.printf("  tags: %s%n", entry.tags());
                    }
                    if (entry.attributes() != null && !entry.attributes().isEmpty()) {
                        out.printf("  attributes: %s%n", entry.attributes());
                    }
                }
            }
            out.flush();
        }
    }
}

@CommandLine.Command(name = "prebuffer", description = "Prebuffer a view or whole profile")
class CMD_vectordata_prebuffer implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_prebuffer.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, description = "Profile name (default: first or preset)")
    private String profile;

    @CommandLine.Option(names = {"-v", "--view"}, description = "View name to prebuffer (default: all views in profile)")
    private String viewName;

    @CommandLine.Option(names = {"--range"}, description = "Byte or record range start:end (optional, view-specific)")
    private String rangeSpec;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                String pname = chooseProfile(session, profile);
                TestDataView tdv = session.selector().profile(pname);
                if (viewName == null || viewName.isBlank()) {
                    tdv.prebuffer().join();
                    spec.commandLine().getOut().printf("Prebuffered all available views for profile '%s'%n", pname);
                } else {
                    var maybeView = ViewSelector.resolve(tdv, viewName);
                    if (maybeView.isEmpty()) {
                        spec.commandLine().getErr().printf("View '%s' not found in profile '%s'%n", viewName, pname);
                        return 1;
                    }
                    DatasetView<?> dv = maybeView.get();
                    if (rangeSpec != null && !rangeSpec.isBlank()) {
                        long[] range = parseRange(rangeSpec);
                        dv.prebuffer(range[0], range[1]).join();
                        spec.commandLine().getOut().printf("Prebuffered %s [%d,%d) in profile '%s'%n",
                            ViewSelector.canonicalName(dv), range[0], range[1], pname);
                    } else {
                        dv.prebuffer().join();
                        spec.commandLine().getOut().printf("Prebuffered %s for profile '%s'%n",
                            ViewSelector.canonicalName(dv), pname);
                    }
                }
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to prebuffer: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("prebuffer failed", e);
            return 1;
        }
    }

    private String chooseProfile(VectordataCliSupport.DatasetSession session, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }
        return session.profileNames().get(0);
    }

    private long[] parseRange(String spec) {
        String[] parts = spec.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Range must be start:end");
        }
        long start = Long.parseLong(parts[0]);
        long end = Long.parseLong(parts[1]);
        if (end < start) {
            throw new IllegalArgumentException("Range end must be >= start");
        }
        return new long[]{start, end};
    }
}

@CommandLine.Command(name = "info", description = "Summarize dataset, profiles, and attachments")
class CMD_vectordata_info implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_info.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                printInfo(spec.commandLine().getOut(), session, format);
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to load dataset: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("info failed", e);
            return 1;
        }
    }

    static void printInfo(PrintWriter out, VectordataCliSupport.DatasetSession session, String format) throws java.io.IOException {
        if ("json".equalsIgnoreCase(format)) {
            java.util.Map<String, Object> payload = new java.util.LinkedHashMap<>();
            payload.put("dataset", session.name());
            payload.put("source", session.source());
            payload.put("profiles", session.profileNames());
            VectordataCliSupport.writeJson(out, payload);
        } else {
            out.printf("Dataset: %s%n", session.name());
            out.printf("Source : %s%n", session.source());
            out.printf("Profiles (%d): %s%n", session.profileNames().size(), String.join(", ", session.profileNames()));
            out.flush();
        }
    }
}

@CommandLine.Command(name = "views", description = "List views per profile")
class CMD_vectordata_views implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_views.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, split = ",", description = "Profile filter (default: all)")
    private List<String> profiles = new ArrayList<>();

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                printViews(spec.commandLine().getOut(), session, profiles, format);
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to list views: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("views failed", e);
            return 1;
        }
    }

    static void printViews(PrintWriter out, VectordataCliSupport.DatasetSession session, List<String> filter, String format) throws java.io.IOException {
        Set<String> requested = (filter == null || filter.isEmpty())
            ? null
            : new LinkedHashSet<>(filter);
        if ("json".equalsIgnoreCase(format)) {
            java.util.List<java.util.Map<String, Object>> profilesJson = new java.util.ArrayList<>();
            for (String pname : session.profileNames()) {
                if (requested != null && !requested.contains(pname)) {
                    continue;
                }
                java.util.Map<String, Object> entry = new java.util.LinkedHashMap<>();
                TestDataView tdv = session.selector().profile(pname);
                java.util.List<String> views = ViewSelector.availableViews(tdv);
                entry.put("profile", pname);
                entry.put("views", views);
                profilesJson.add(entry);
            }
            VectordataCliSupport.writeJson(out, profilesJson);
            return;
        }
        for (String pname : session.profileNames()) {
            if (requested != null && !requested.contains(pname)) {
                continue;
            }
            out.printf("Profile: %s%n", pname);
            TestDataView view = session.selector().profile(pname);
            emitView(out, "base_vectors", view.getBaseVectors().orElse(null));
            emitView(out, "query_vectors", view.getQueryVectors().orElse(null));
            emitView(out, "neighbor_indices", view.getNeighborIndices().orElse(null));
            emitView(out, "neighbor_distances", view.getNeighborDistances().orElse(null));
            out.println();
        }
        out.flush();
    }

    private static void emitView(PrintWriter out, String name, Object maybeView) {
        if (maybeView == null) {
            return;
        }
        if (maybeView instanceof BaseVectors bv) {
            out.printf("  %-18s count=%d dim=%d%n", name, bv.getCount(), bv.getVectorDimensions());
        } else if (maybeView instanceof QueryVectors qv) {
            out.printf("  %-18s count=%d dim=%d%n", name, qv.getCount(), qv.getVectorDimensions());
        } else if (maybeView instanceof NeighborIndices ni) {
            out.printf("  %-18s count=%d k=%d%n", name, ni.getCount(), ni.getVectorDimensions());
        } else if (maybeView instanceof NeighborDistances nd) {
            out.printf("  %-18s count=%d k=%d%n", name, nd.getCount(), nd.getVectorDimensions());
        } else {
            out.printf("  %-18s available%n", name);
        }
    }
}

@CommandLine.Command(name = "profiles", description = "List profile names")
class CMD_vectordata_profiles implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_profiles.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                if ("json".equalsIgnoreCase(format)) {
                    VectordataCliSupport.writeJson(spec.commandLine().getOut(), session.profileNames());
                } else {
                    spec.commandLine().getOut().printf("Profiles (%d): %s%n", session.profileNames().size(),
                        String.join(", ", session.profileNames()));
                }
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to list profiles: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("profiles failed", e);
            return 1;
        }
    }
}

@CommandLine.Command(name = "size", description = "Show counts/dimensions for a view")
class CMD_vectordata_size implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_size.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, description = "Profile name (default: first or preset)")
    private String profile;

    @CommandLine.Option(names = {"-v", "--view"}, defaultValue = "base_vectors", description = "View name (base_vectors, query_vectors, neighbor_indices, neighbor_distances)")
    private String viewName;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                String pname = chooseProfile(session, profile);
                TestDataView tdv = session.selector().profile(pname);
                var maybeView = ViewSelector.resolve(tdv, viewName);
                if (maybeView.isEmpty()) {
                    spec.commandLine().getErr().printf("View '%s' not found in profile '%s'%n", viewName, pname);
                    return 1;
                }
                DatasetView<?> dv = maybeView.get();
                emitSize(spec.commandLine().getOut(), session.name(), pname, dv, format);
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to size view: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("size failed", e);
            return 1;
        }
    }

    void emitSize(PrintWriter out, String dataset, String profile, DatasetView<?> dv, String format) throws java.io.IOException {
        String cname = ViewSelector.canonicalName(dv);
        if ("json".equalsIgnoreCase(format)) {
            java.util.Map<String, Object> payload = new java.util.LinkedHashMap<>();
            payload.put("dataset", dataset);
            payload.put("profile", profile);
            payload.put("view", cname);
            payload.put("count", dv.getCount());
            payload.put("dim_or_k", dv.getVectorDimensions());
            payload.put("type", dv.getDataType().getSimpleName());
            if (dv instanceof NeighborIndices ni) {
                payload.put("maxk", ni.getMaxK());
            }
            VectordataCliSupport.writeJson(out, payload);
            return;
        }
        out.printf("Dataset: %s%n", dataset);
        out.printf("Profile: %s%n", profile);
        out.printf("View   : %s%n", cname);
        out.printf("Count  : %d%n", dv.getCount());
        out.printf("Dim/K  : %d%n", dv.getVectorDimensions());
        out.printf("Type   : %s%n", dv.getDataType().getSimpleName());
        if (dv instanceof NeighborIndices ni) {
            out.printf("MaxK   : %d%n", ni.getMaxK());
        }
        out.flush();
    }

    private String chooseProfile(VectordataCliSupport.DatasetSession session, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }
        return session.profileNames().get(0);
    }
}

@CommandLine.Command(name = "sample", description = "Sample vectors from a view")
class CMD_vectordata_sample implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_sample.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, description = "Profile name (default: first or preset)")
    private String profile;

    @CommandLine.Option(names = {"-v", "--view"}, defaultValue = "base_vectors", description = "View name (base_vectors, query_vectors, neighbor_indices, neighbor_distances)")
    private String viewName;

    @CommandLine.Option(names = {"-s", "--start"}, defaultValue = "0", description = "Start index")
    private long start;

    @CommandLine.Option(names = {"-c", "--count"}, defaultValue = "5", description = "Number of records to show")
    private int count;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|json")
    private String format;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                String pname = chooseProfile(session, profile);
                TestDataView tdv = session.selector().profile(pname);
                var maybeView = ViewSelector.resolve(tdv, viewName);
                if (maybeView.isEmpty()) {
                    spec.commandLine().getErr().printf("View '%s' not found in profile '%s'%n", viewName, pname);
                    return 1;
                }
                DatasetView<?> dv = maybeView.get();
                emitSample(spec.commandLine().getOut(), dv, start, count, format);
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to sample view: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("sample failed", e);
            return 1;
        }
    }

    void emitSample(PrintWriter out, DatasetView<?> dv, long start, int count, String format) throws java.io.IOException {
        long endExclusive = Math.min(start + count, dv.getCount());
        if ("json".equalsIgnoreCase(format)) {
            java.util.List<Object> rows = new java.util.ArrayList<>();
            for (long i = start; i < endExclusive; i++) {
                rows.add(java.util.Map.of("index", i, "value", dv.get(i)));
            }
            VectordataCliSupport.writeJson(out, rows);
        } else {
            for (long i = start; i < endExclusive; i++) {
                Object val = dv.get(i);
                out.printf("[%d] %s%n", i, render(val));
            }
            out.flush();
        }
    }

    private String render(Object val) {
        if (val == null) {
            return "null";
        }
        if (val instanceof float[] fa) {
            return java.util.Arrays.toString(fa);
        } else if (val instanceof int[] ia) {
            return java.util.Arrays.toString(ia);
        } else if (val instanceof double[] da) {
            return java.util.Arrays.toString(da);
        } else if (val instanceof Object[] oa) {
            return java.util.Arrays.toString(oa);
        }
        return val.toString();
    }

    private String chooseProfile(VectordataCliSupport.DatasetSession session, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }
        return session.profileNames().get(0);
    }
}

@CommandLine.Command(name = "cat", description = "Stream a range of vectors to stdout")
class CMD_vectordata_cat implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_cat.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, description = "Profile name (default: first or preset)")
    private String profile;

    @CommandLine.Option(names = {"-v", "--view"}, defaultValue = "base_vectors", description = "View name (base_vectors, query_vectors, neighbor_indices, neighbor_distances)")
    private String viewName;

    @CommandLine.Option(names = {"-s", "--start"}, defaultValue = "0", description = "Start index")
    private long start;

    @CommandLine.Option(names = {"-e", "--end"}, description = "End index (exclusive). If unset, uses start+count or view length.")
    private Long end;

    @CommandLine.Option(names = {"-c", "--count"}, description = "Number of records to emit (alternative to --end)")
    private Integer count;

    @CommandLine.Option(names = {"--format"}, defaultValue = "text", description = "Output format: text|raw|json")
    private String format;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                String pname = chooseProfile(session, profile);
                TestDataView tdv = session.selector().profile(pname);
                var maybeView = ViewSelector.resolve(tdv, viewName);
                if (maybeView.isEmpty()) {
                    spec.commandLine().getErr().printf("View '%s' not found in profile '%s'%n", viewName, pname);
                    return 1;
                }
                DatasetView<?> dv = maybeView.get();
                long endIdx = resolveEnd(dv, start, end, count);
                if (start >= dv.getCount()) {
                    spec.commandLine().getErr().printf("Start index %d is >= view size %d%n", start, dv.getCount());
                    return 1;
                }
                if ("json".equalsIgnoreCase(format)) {
                    java.util.List<Object> rows = new java.util.ArrayList<>();
                    for (long i = start; i < endIdx; i++) {
                        rows.add(java.util.Map.of("index", i, "value", dv.get(i)));
                    }
                    VectordataCliSupport.writeJson(spec.commandLine().getOut(), rows);
                } else {
                    for (long i = start; i < endIdx; i++) {
                        Object val = dv.get(i);
                        if ("raw".equalsIgnoreCase(format)) {
                            writeRaw(dv, val, System.out);
                        } else {
                            spec.commandLine().getOut().println(render(val));
                        }
                    }
                    spec.commandLine().getOut().flush();
                }
            }
            return 0;
        } catch (Exception e) {
            String msg = "Failed to cat view: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("cat failed", e);
            return 1;
        }
    }

    private long resolveEnd(DatasetView<?> dv, long start, Long end, Integer count) {
        if (end != null) {
            return Math.min(end, dv.getCount());
        }
        if (count != null) {
            return Math.min(start + count, dv.getCount());
        }
        return dv.getCount();
    }

    private String chooseProfile(VectordataCliSupport.DatasetSession session, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }
        return session.profileNames().get(0);
    }

    static String render(Object val) {
        if (val == null) {
            return "null";
        }
        if (val instanceof float[] fa) {
            return java.util.Arrays.toString(fa);
        } else if (val instanceof int[] ia) {
            return java.util.Arrays.toString(ia);
        } else if (val instanceof double[] da) {
            return java.util.Arrays.toString(da);
        } else if (val instanceof Object[] oa) {
            return java.util.Arrays.toString(oa);
        }
        return val.toString();
    }

    private void writeRaw(DatasetView<?> dv, Object val, java.io.OutputStream out) throws Exception {
        if (val instanceof float[] fa) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(fa.length * 4).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (float f : fa) {
                buf.putFloat(f);
            }
            out.write(buf.array());
        } else if (val instanceof int[] ia) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(ia.length * 4).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (int v : ia) {
                buf.putInt(v);
            }
            out.write(buf.array());
        } else if (val instanceof double[] da) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(da.length * 8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (double v : da) {
                buf.putDouble(v);
            }
            out.write(buf.array());
        } else if (val instanceof Indexed<?> idx) {
            writeRaw(dv, idx.value(), out);
        } else if (val instanceof Object[] oa && oa.length > 0 && oa[0] instanceof Number) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(oa.length * 8).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (Object o : oa) {
                buf.putDouble(((Number) o).doubleValue());
            }
            out.write(buf.array());
        } else {
            // fallback to text if type unsupported
            spec.commandLine().getOut().println(render(val));
        }
    }
}

@CommandLine.Command(name = "verify", description = "Verify availability by prebuffering a view or profile")
class CMD_vectordata_verify implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_verify.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, required = true, description = "dataset.yaml path or URL")
    private String datasetSpec;

    @CommandLine.Option(names = {"-p", "--profile"}, description = "Profile name (default: first or preset)")
    private String profile;

    @CommandLine.Option(names = {"-v", "--view"}, description = "View name to verify (default: all views in profile)")
    private String viewName;

    @CommandLine.Option(names = {"--cache"}, description = "Cache directory for remote datasets")
    private String cacheDir;

    @Override
    public Integer call() {
        VectordataCliSupport.logInvocation(spec, logger);
        try {
            try (VectordataCliSupport.DatasetSession session = VectordataCliSupport.loadDataset(datasetSpec, cacheDir)) {
                String pname = chooseProfile(session, profile);
                TestDataView tdv = session.selector().profile(pname);
                if (viewName == null || viewName.isBlank()) {
                    tdv.prebuffer().join();
                    spec.commandLine().getOut().printf("Verified all views for profile '%s'%n", pname);
                } else {
                    var maybeView = ViewSelector.resolve(tdv, viewName);
                    if (maybeView.isEmpty()) {
                        spec.commandLine().getErr().printf("View '%s' not found in profile '%s'%n", viewName, pname);
                        return 1;
                    }
                    DatasetView<?> dv = maybeView.get();
                    dv.prebuffer().join();
                    spec.commandLine().getOut().printf("Verified view '%s' for profile '%s'%n",
                        ViewSelector.canonicalName(dv), pname);
                }
                spec.commandLine().getOut().flush();
            }
            return 0;
        } catch (Exception e) {
            String msg = "Verify failed: " + e.getMessage();
            spec.commandLine().getErr().println(msg);
            logger.debug("verify failed", e);
            return 1;
        }
    }

    private String chooseProfile(VectordataCliSupport.DatasetSession session, String requested) {
        if (requested != null && !requested.isBlank()) {
            return requested;
        }
        return session.profileNames().get(0);
    }
}

@CommandLine.Command(name = "repl", description = "Interactive vectordata explorer")
class CMD_vectordata_repl implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_vectordata_repl.class);
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataset"}, description = "Initial dataset.yaml path or URL")
    private String datasetSpec;

    private final java.util.List<String> replCommands = java.util.List.of(
        "use", "info", "datasets", "profiles", "views", "size", "sample", "prebuffer", "cat", "verify", "help", "exit", "quit", "q"
    );

    @Override
    public Integer call() {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();
        VectordataCliSupport.DatasetSession current = null;
        if (datasetSpec != null) {
            try {
                current = VectordataCliSupport.loadDataset(datasetSpec);
            } catch (Exception e) {
                err.printf("Failed to load initial dataset: %s%n", e.getMessage());
                err.flush();
            }
        }

        out.println("vectordata repl. commands: use <uri>, info, profiles, views [profile,...], size, sample, prebuffer, cat, verify, help, exit");
        out.flush();
        try {
            return runJLineRepl(out, err, current);
        } finally {
            if (current != null) {
                current.close();
            }
        }
    }

    private Integer runJLineRepl(PrintWriter out, PrintWriter err, VectordataCliSupport.DatasetSession current) {
        final VectordataCliSupport.DatasetSession[] cur = new VectordataCliSupport.DatasetSession[]{current};
        try {
            org.jline.reader.LineReader reader = org.jline.reader.LineReaderBuilder.builder()
                .completer((org.jline.reader.Completer) (ln, parsed, candidates) -> {
                    String word = parsed.word();
                    int index = parsed.words().size() - 1;
                    java.util.List<String> views = java.util.Collections.emptyList();
                    java.util.List<String> profiles = java.util.Collections.emptyList();
                    if (cur[0] != null) {
                        profiles = cur[0].profileNames();
                        try {
                            TestDataView tdv = cur[0].selector().profile(profiles.get(0));
                            views = ViewSelector.availableViews(tdv);
                        } catch (Exception ignored) {}
                    }
                    if (index <= 0) {
                        replCommands.stream()
                            .filter(c -> word == null || c.startsWith(word.toLowerCase()))
                            .forEach(c -> candidates.add(new Candidate(c)));
                    } else {
                        String cmd = parsed.words().get(0).toLowerCase();
                        if (java.util.Set.of("views", "prebuffer", "size", "sample", "cat", "verify").contains(cmd)) {
                            if (index == 1) {
                                profiles.forEach(p -> candidates.add(new Candidate(p)));
                                views.forEach(v -> candidates.add(new Candidate(v)));
                            } else if (index == 2) {
                                views.forEach(v -> candidates.add(new Candidate(v)));
                            }
                        } else if ("use".equals(cmd) && index == 1) {
                            // no dynamic completion
                        }
                    }
                })
                .build();

            while (true) {
                String line;
                try {
                    line = reader.readLine("vd> ");
                } catch (org.jline.reader.EndOfFileException eof) {
                    break;
                } catch (org.jline.reader.UserInterruptException uie) {
                    break;
                }

                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] args = VectordataCliSupport.tokenize(line);
                String cmd = args[0];
                if ("exit".equalsIgnoreCase(cmd) || "quit".equalsIgnoreCase(cmd) || "q".equalsIgnoreCase(cmd)) {
                    break;
                } else if ("help".equalsIgnoreCase(cmd)) {
                    out.println("use <dataset>     load a dataset.yaml (file or URL)");
                    out.println("info              summarize current dataset");
                    out.println("profiles          list profile names");
                    out.println("views [profiles]  list views for profiles (comma or space separated)");
                    out.println("size [opts]       show count/dim for a view");
                    out.println("sample [opts]     print a few records");
                    out.println("prebuffer [opts]  prebuffer a view or profile");
                    out.println("cat [opts]        stream records");
                    out.println("verify [opts]     prebuffer as a verification pass");
                    out.println("quit|exit         leave the repl");
                    out.flush();
                } else if ("use".equalsIgnoreCase(cmd)) {
                    if (args.length < 2) {
                        out.println("usage: use <dataset.yaml path or URL>");
                        out.flush();
                        continue;
                    }
                    if (cur[0] != null) {
                        cur[0].close();
                    }
                    try {
                        cur[0] = VectordataCliSupport.loadDataset(args[1]);
                        out.printf("Loaded dataset '%s' (%s)%n", cur[0].name(), cur[0].source());
                        out.flush();
                    } catch (Exception e) {
                        err.printf("Failed to load dataset: %s%n", e.getMessage());
                        err.flush();
                    }
                } else if ("info".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    CMD_vectordata_info.printInfo(out, cur[0], "text");
                } else if ("profiles".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    out.printf("Profiles (%d): %s%n", cur[0].profileNames().size(),
                        String.join(", ", cur[0].profileNames()));
                    out.flush();
                } else if ("datasets".equalsIgnoreCase(cmd)) {
                    try {
                        TestDataSources sources = new TestDataSources().configure(java.nio.file.Path.of(System.getProperty("user.home"), ".config", "vectordata"));
                        Catalog catalog = Catalog.of(sources);
                        CMD_vectordata_datasets.printDatasets(out, catalog.datasets(), "text", false);
                    } catch (Exception e) {
                        err.printf("Failed to list datasets: %s%n", e.getMessage());
                        err.flush();
                    }
                } else if ("views".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    List<String> pfilter = new ArrayList<>();
                    if (args.length > 1) {
                        for (int i = 1; i < args.length; i++) {
                            for (String part : args[i].split(",")) {
                                if (!part.isBlank()) {
                                    pfilter.add(part);
                                }
                            }
                        }
                    }
                    CMD_vectordata_views.printViews(out, cur[0], pfilter, "text");
                } else if ("size".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    String v = args.length > 1 ? args[1] : "base_vectors";
                    String p = cur[0].profileNames().get(0);
                    TestDataView tdv = cur[0].selector().profile(p);
                    var maybeView = ViewSelector.resolve(tdv, v);
                    if (maybeView.isEmpty()) {
                        out.printf("View '%s' not found in profile '%s'%n", v, p);
                    } else {
                        new CMD_vectordata_size().emitSize(out, cur[0].name(), p, maybeView.get(), "text");
                    }
                    out.flush();
                } else if ("sample".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    String v = args.length > 1 ? args[1] : "base_vectors";
                    long s = args.length > 2 ? Long.parseLong(args[2]) : 0;
                    int c = args.length > 3 ? Integer.parseInt(args[3]) : 5;
                    String p = cur[0].profileNames().get(0);
                    TestDataView tdv = cur[0].selector().profile(p);
                    var maybeView = ViewSelector.resolve(tdv, v);
                    if (maybeView.isEmpty()) {
                        out.printf("View '%s' not found in profile '%s'%n", v, p);
                    } else {
                        new CMD_vectordata_sample().emitSample(out, maybeView.get(), s, c, "text");
                    }
                    out.flush();
                } else if ("prebuffer".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    String v = args.length > 1 ? args[1] : null;
                    String p = cur[0].profileNames().get(0);
                    TestDataView tdv = cur[0].selector().profile(p);
                    if (v == null) {
                        tdv.prebuffer().join();
                        out.printf("Prebuffered all views for profile '%s'%n", p);
                    } else {
                        var mv = ViewSelector.resolve(tdv, v);
                        if (mv.isEmpty()) {
                            out.printf("View '%s' not found in profile '%s'%n", v, p);
                        } else {
                            mv.get().prebuffer().join();
                            out.printf("Prebuffered view '%s' in profile '%s'%n", v, p);
                        }
                    }
                    out.flush();
                } else if ("cat".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    String v = args.length > 1 ? args[1] : "base_vectors";
                    long s = args.length > 2 ? Long.parseLong(args[2]) : 0;
                    int c = args.length > 3 ? Integer.parseInt(args[3]) : 5;
                    String p = cur[0].profileNames().get(0);
                    TestDataView tdv = cur[0].selector().profile(p);
                    var maybeView = ViewSelector.resolve(tdv, v);
                    if (maybeView.isEmpty()) {
                        out.printf("View '%s' not found in profile '%s'%n", v, p);
                    } else {
                        DatasetView<?> dv = maybeView.get();
                        long end = Math.min(s + c, dv.getCount());
                        for (long i = s; i < end; i++) {
                            out.println(CMD_vectordata_cat.render(dv.get(i)));
                        }
                    }
                    out.flush();
                } else if ("verify".equalsIgnoreCase(cmd)) {
                    if (cur[0] == null) {
                        out.println("No dataset loaded. use <uri> first.");
                        out.flush();
                        continue;
                    }
                    String v = args.length > 1 ? args[1] : null;
                    String p = cur[0].profileNames().get(0);
                    TestDataView tdv = cur[0].selector().profile(p);
                    if (v == null) {
                        tdv.prebuffer().join();
                        out.printf("Verified all views for profile '%s'%n", p);
                    } else {
                        var mv = ViewSelector.resolve(tdv, v);
                        if (mv.isEmpty()) {
                            out.printf("View '%s' not found in profile '%s'%n", v, p);
                        } else {
                            mv.get().prebuffer().join();
                            out.printf("Verified view '%s' in profile '%s'%n", v, p);
                        }
                    }
                    out.flush();
                } else {
                    out.printf("Unknown command '%s'. Try 'help'.%n", cmd);
                    out.flush();
                }
            }
        } catch (Exception e) {
            err.printf("REPL error: %s%n", e.getMessage());
            logger.debug("repl error", e);
            return 1;
        }
        return 0;
    }
}
