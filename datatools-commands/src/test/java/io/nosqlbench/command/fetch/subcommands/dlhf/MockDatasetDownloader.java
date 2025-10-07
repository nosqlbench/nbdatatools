package io.nosqlbench.command.fetch.subcommands.dlhf;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A factory for creating mock DatasetDownloader instances for testing purposes.
 * This allows tests to run without making actual HTTP requests.
 */
public class MockDatasetDownloader {

    private final Path targetDir;

    /**
     * Creates a new mock dataset downloader factory
     * 
     * @param targetDir Target directory to save downloaded files
     */
    public MockDatasetDownloader(Path targetDir) {
        this.targetDir = targetDir;
    }

    /**
     * Creates a mock file in the target directory to simulate a successful download
     * 
     * @throws IOException If the file cannot be created
     */
    public void createMockDownload() throws IOException {
        // Create the target directory if it doesn't exist
        Files.createDirectories(targetDir);

        // Create a mock file to simulate a successful download
        Path mockFile = targetDir.resolve("mock_file.parquet");
        Files.writeString(mockFile, "This is a mock file for testing purposes");
    }
}
