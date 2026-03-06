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

import io.nosqlbench.command.common.DatasetCompletionCandidates;
import io.nosqlbench.vectordata.config.VectorDataSettings;
import io.nosqlbench.vectordata.discovery.vector.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.layoutv2.DSInterval;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.layoutv2.DSSource;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.datasets.types.ViewKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;

/// Prebuffer datasets from accessible catalogs
@CommandLine.Command(name = "prebuffer",
    header = "Prebuffer datasets from accessible catalogs",
    description = "Downloads and prebuffers vector testing datasets from accessible catalogs",
    exitCodeList = {"0: success", "1: error"})
public class CMD_datasets_prebuffer implements Callable<Integer> {

    /// Creates a new CMD_datasets_prebuffer instance.
    public CMD_datasets_prebuffer() {
    }

    private static final Logger logger = LogManager.getLogger(CMD_datasets_prebuffer.class);

    @CommandLine.Parameters(description = "Dataset and profile to prebuffer using 'dataset:profile'. Escape literal ':' with '\\:'.",
                           arity = "1..*",
                           completionCandidates = DatasetCompletionCandidates.DatasetProfile.class)
    private List<String> datasets = new ArrayList<>();

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

    @CommandLine.Option(names = {"--verbose", "-v"}, description = "Show more information")
    private boolean verbose = false;

    @CommandLine.Option(names = {"--progress"}, description = "Show progress information", defaultValue = "true")
    private boolean showProgress = true;
    
    @CommandLine.Option(names = {"--views"}, 
        description = "Comma-separated list of views to prebuffer (base_vectors, query_vectors, neighbor_indices, neighbor_distances). Use '*' for all views.",
        defaultValue = "*")
    private String views = "*";

    @Override
    public Integer call() {
        try {
            this.configdir = expandPath(this.configdir);
            this.cacheDir = resolveCacheDir();
            if (this.cacheDir == null) {
                return 1;
            }
            
            TestDataSources config = new TestDataSources().configure(this.configdir);
            config = config.addCatalogs(this.catalogs);
            
            Catalog catalog = Catalog.of(config);
            
            for (String rawSpec : datasets) {
                DatasetProfileSpec spec;
                try {
                    spec = DatasetProfileSpec.parse(rawSpec);
                } catch (IllegalArgumentException iae) {
                    System.err.println("Invalid dataset specification '" + rawSpec + "': " + iae.getMessage());
                    return 1;
                }

                if (spec.profile().isEmpty()) {
                    System.err.println("Dataset specification must include a profile (e.g. dataset:profile). Got: " + rawSpec);
                    return 1;
                }

                if (prebufferDataset(catalog, spec) != 0) {
                    return 1;
                }
            }
            
            return 0;
            
        } catch (Exception e) {
            System.err.println("Error during prebuffer operation: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return 1;
        }
    }
    
    private int prebufferDataset(Catalog catalog, DatasetProfileSpec spec) {
        String profileName = spec.profile().orElseThrow();
        try {
            // Use the standard ProfileSelector API
            ProfileSelector profileSelector = catalog.select(spec)
                .setCacheDir(cacheDir.toString());

            // Get the TestDataView using the standard profile() method
            TestDataView vectorTestDataView = profileSelector.profile(profileName);

            System.out.println("Prebuffering dataset: " + spec.dataset() + ":" + profileName);
            if (verbose) {
                System.out.println("Test data view: " + vectorTestDataView.getName());
                System.out.println("URL: " + vectorTestDataView.getUrl());
            }
            
            Instant startTime = Instant.now();
            
            // Parse the views to prebuffer
            List<String> viewsToPrebuffer = parseViews();

            // Print summary before prebuffering
            printPrebufferPlan(catalog, spec, viewsToPrebuffer);
            
            // Prebuffer based on selected views
            CompletableFuture<Void> prebufferFuture;
            if (viewsToPrebuffer.contains("*")) {
                // Prebuffer all views
                prebufferFuture = vectorTestDataView.prebuffer();
                System.out.println("Prebuffering all views");
            } else {
                // Prebuffer only selected views
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                
                if (viewsToPrebuffer.contains("base_vectors")) {
                    vectorTestDataView.getBaseVectors().ifPresent(baseVectors -> {
                        System.out.println("Prebuffering base_vectors");
                        futures.add(baseVectors.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("query_vectors")) {
                    vectorTestDataView.getQueryVectors().ifPresent(queryVectors -> {
                        System.out.println("Prebuffering query_vectors");
                        futures.add(queryVectors.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("neighbor_indices")) {
                    vectorTestDataView.getNeighborIndices().ifPresent(neighborIndices -> {
                        System.out.println("Prebuffering neighbor_indices");
                        futures.add(neighborIndices.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("neighbor_distances")) {
                    vectorTestDataView.getNeighborDistances().ifPresent(neighborDistances -> {
                        System.out.println("Prebuffering neighbor_distances");
                        futures.add(neighborDistances.prebuffer());
                    });
                }
                
                if (futures.isEmpty()) {
                    System.out.println("No matching views found to prebuffer");
                    return 0;
                }
                
                prebufferFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            }
            
            if (showProgress) {
                showProgressUntilComplete(prebufferFuture, startTime);
            } else {
                prebufferFuture.join();
            }
            
            Duration totalTime = Duration.between(startTime, Instant.now());
            System.out.println("\nPrebuffer completed in " + formatDuration(totalTime));
            
            return 0;
            
        } catch (Exception e) {
            System.err.println("Error prebuffering dataset " + spec.dataset() + ":" + profileName + ": " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return 1;
        }
    }
    
    private void showProgressUntilComplete(CompletableFuture<Void> future, Instant startTime) {
        System.out.print("Progress: ");
        
        while (!future.isDone()) {
            try {
                Thread.sleep(1000); // Update every second
                Duration elapsed = Duration.between(startTime, Instant.now());
                System.out.print(".");
                System.out.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        try {
            future.join(); // Wait for completion and handle any exceptions
        } catch (Exception e) {
            System.err.println("\nPrebuffer failed: " + e.getMessage());
            throw e;
        }
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
    
    private String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) return seconds + "s";
        if (seconds < 3600) return String.format("%dm %ds", seconds / 60, seconds % 60);
        return String.format("%dh %dm %ds", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    }
    
    private List<String> parseViews() {
        List<String> viewList = new ArrayList<>();
        
        if (views == null || views.trim().isEmpty() || views.equals("*")) {
            viewList.add("*");
            return viewList;
        }

        // Split by comma and trim whitespace
        String[] viewArray = views.split(",");
        for (String view : viewArray) {
            String trimmedView = view.trim().toLowerCase();
            if (trimmedView.isEmpty()) {
                continue;
            }

            if (trimmedView.equals("*")) {
                viewList.clear();
                viewList.add("*");
                break;
            }

            TestDataKind.fromOptionalString(trimmedView)
                .ifPresentOrElse(kind -> viewList.add(kind.name().toLowerCase()),
                    () -> System.err.println("Warning: Unknown view name '" + trimmedView + "', ignoring"));
        }

        // If no valid views were found, default to all
        if (viewList.isEmpty()) {
            viewList.add("*");
        }

        return viewList;
    }

    private void printPrebufferPlan(Catalog catalog, DatasetProfileSpec spec, List<String> viewsToPrebuffer) {
        String profileName = spec.profile().orElse("");
        DatasetEntry dataset = catalog.findExact(spec.dataset()).orElse(null);
        if (dataset == null) {
            System.out.println("Prebuffer plan: dataset not found for " + spec.dataset());
            return;
        }
        DSProfile profile = resolveProfile(dataset, profileName);
        if (profile == null) {
            System.out.println("Prebuffer plan: profile '" + profileName + "' not found in dataset " + dataset.name());
            return;
        }

        System.out.println("Prebuffer plan for " + dataset.name() + ":" + profileName);
        List<ViewKind> viewKinds = resolveViewKinds(viewsToPrebuffer);
        for (ViewKind viewKind : viewKinds) {
            DSView view = findView(profile, viewKind);
            if (view == null) {
                continue;
            }
            DSSource source = view.getSource();
            String sourcePath = (source != null && source.getPath() != null) ? source.getPath() : "unknown";
            DSWindow window = selectWindow(view, source);
            String windowLabel = formatWindow(window);
            System.out.println("  " + viewKind.getDatasetKind().name().toLowerCase() + ": " +
                sourcePath + " (window " + windowLabel + ")");
        }
    }

    private DSProfile resolveProfile(DatasetEntry dataset, String profileName) {
        if (profileName == null) {
            return null;
        }
        DSProfile profile = dataset.profiles().get(profileName);
        if (profile == null) {
            profile = dataset.profiles().get(profileName.toLowerCase());
        }
        if (profile == null) {
            profile = dataset.profiles().get(profileName.toUpperCase());
        }
        if (profile == null) {
            for (String key : dataset.profiles().keySet()) {
                if (key.equalsIgnoreCase(profileName)) {
                    profile = dataset.profiles().get(key);
                    break;
                }
            }
        }
        return profile;
    }

    private List<ViewKind> resolveViewKinds(List<String> viewsToPrebuffer) {
        List<ViewKind> viewKinds = new ArrayList<>();
        if (viewsToPrebuffer.contains("*")) {
            for (ViewKind kind : ViewKind.values()) {
                viewKinds.add(kind);
            }
            return viewKinds;
        }
        for (String view : viewsToPrebuffer) {
            ViewKind.fromName(view).ifPresent(viewKinds::add);
        }
        return viewKinds;
    }

    private DSView findView(DSProfile profile, ViewKind viewKind) {
        for (String viewName : profile.keySet()) {
            String normalized = viewName.toLowerCase();
            if (viewKind.getAllNames().contains(normalized)) {
                return profile.get(viewName);
            }
            DSView dsView = profile.get(viewName);
            if (dsView != null && dsView.getName() != null) {
                String dsViewName = dsView.getName().toLowerCase();
                if (viewKind.getAllNames().contains(dsViewName)) {
                    return dsView;
                }
            }
        }
        return null;
    }

    private DSWindow selectWindow(DSView view, DSSource source) {
        if (view != null && view.getWindow() != null && !view.getWindow().isEmpty()) {
            return view.getWindow();
        }
        if (source != null && source.getWindow() != null && !source.getWindow().isEmpty()) {
            return source.getWindow();
        }
        return null;
    }

    private String formatWindow(DSWindow window) {
        if (window == null || window.isEmpty()) {
            return "all";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (DSInterval interval : window) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append('[').append(interval.getMinIncl()).append(',')
                .append(interval.getMaxExcl()).append(')');
        }
        return sb.toString();
    }
}
