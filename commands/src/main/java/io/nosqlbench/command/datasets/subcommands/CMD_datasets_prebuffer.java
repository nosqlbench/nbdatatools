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

import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
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

    private static final Logger logger = LogManager.getLogger(CMD_datasets_prebuffer.class);

    @CommandLine.Parameters(description = "Dataset and profile to prebuffer using 'dataset:profile'. Escape literal ':' with '\\:'.",
                           arity = "1..*")
    private List<String> datasets = new ArrayList<>();

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--cache-dir"},
        description = "Directory for cached dataset files",
        defaultValue = "./cache")
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
            this.cacheDir = expandPath(this.cacheDir);
            
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
            TestDataView testDataView = profileSelector.profile(profileName);

            System.out.println("Prebuffering dataset: " + spec.dataset() + ":" + profileName);
            if (verbose) {
                System.out.println("Test data view: " + testDataView.getName());
                System.out.println("URL: " + testDataView.getUrl());
            }
            
            Instant startTime = Instant.now();
            
            // Parse the views to prebuffer
            List<String> viewsToPrebuffer = parseViews();
            
            // Prebuffer based on selected views
            CompletableFuture<Void> prebufferFuture;
            if (viewsToPrebuffer.contains("*")) {
                // Prebuffer all views
                prebufferFuture = testDataView.prebuffer();
                System.out.println("Prebuffering all views");
            } else {
                // Prebuffer only selected views
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                
                if (viewsToPrebuffer.contains("base_vectors")) {
                    testDataView.getBaseVectors().ifPresent(baseVectors -> {
                        System.out.println("Prebuffering base_vectors");
                        futures.add(baseVectors.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("query_vectors")) {
                    testDataView.getQueryVectors().ifPresent(queryVectors -> {
                        System.out.println("Prebuffering query_vectors");
                        futures.add(queryVectors.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("neighbor_indices")) {
                    testDataView.getNeighborIndices().ifPresent(neighborIndices -> {
                        System.out.println("Prebuffering neighbor_indices");
                        futures.add(neighborIndices.prebuffer());
                    });
                }
                
                if (viewsToPrebuffer.contains("neighbor_distances")) {
                    testDataView.getNeighborDistances().ifPresent(neighborDistances -> {
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
}
