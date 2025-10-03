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

import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Download datasets from accessible catalogs
@CommandLine.Command(name = "download",
    header = "Download datasets from accessible catalogs",
    description = "Download vector testing datasets from accessible catalogs",
    exitCodeList = {"0: success", "1: error"})
public class CMD_datasets_download implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_datasets_download.class);

    @CommandLine.Parameters(description = "Dataset and profile to download using 'dataset:profile'. Escape literal ':' with '\\:'.",
                           arity = "1..*")
    private List<String> datasets = new ArrayList<>();

    @CommandLine.Option(names = {"--catalog"},
        description = "A directory, remote url, or other catalog container")
    private List<String> catalogs = new ArrayList<>();

    @CommandLine.Option(names = {"--configdir"},
        description = "The directory to use for configuration files",
        defaultValue = "~/.config/vectordata")
    private Path configdir;

    @CommandLine.Option(names = {"--output", "-o"},
        description = "Output directory for downloaded datasets",
        defaultValue = "./datasets")
    private Path outputDir;

    @CommandLine.Option(names = {"--verbose", "-v"}, description = "Show more information")
    private boolean verbose = false;

    @Override
    public Integer call() {
        this.configdir =
            Path.of(this.configdir.toString().replace("~", System.getProperty("user.home"))
                .replace("${HOME}", System.getProperty("user.home")));
                
        TestDataSources config = new TestDataSources().configure(this.configdir);
        config = config.addCatalogs(this.catalogs);

        Catalog catalog = Catalog.of(config);

        // TODO: Implement actual download logic
        System.out.println("Download command not yet implemented");

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

            String profileName = spec.profile().orElseThrow();

            try {
                ProfileSelector selector = catalog.select(spec);
                TestDataView view = selector.profile(profileName);
                System.out.println("Would download: " + view.getName() + " to " + outputDir);
            } catch (IllegalArgumentException iae) {
                System.err.println("Unable to resolve dataset profile '" + rawSpec + "': " + iae.getMessage());
                return 1;
            }
        }

        return 0;
    }
}
