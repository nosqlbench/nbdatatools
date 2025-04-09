package io.nosqlbench.vectordata.download;

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


import io.nosqlbench.vectordata.TestDataGroup;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public record DownloadResult(
    Path path,
    DownloadStatus status,
    long bytes,
    Exception error
) {
    public static DownloadResult downloaded(Path path, long bytes) {
        return new DownloadResult(path, DownloadStatus.DOWNLOADED, bytes, null);
    }

    public static DownloadResult skipped(Path path, long bytes) {
        return new DownloadResult(path, DownloadStatus.SKIPPED, bytes, null);
    }

    public static DownloadResult failed(Path path, Exception error) {
        return new DownloadResult(path, DownloadStatus.FAILED, 0, error);
    }

    public boolean isSuccess() {
        return status == DownloadStatus.DOWNLOADED || status == DownloadStatus.SKIPPED;
    }

    public Optional<TestDataGroup> getDataGroup() {
        if (isSuccess()) {
            return Optional.of(new TestDataGroup(path));
        } else {
            return Optional.empty();
        }

    }
    public TestDataGroup getRequiredDataGroup() {
        return getDataGroup().orElseThrow(() -> new RuntimeException("download of '" + path +
                                                                     "' failed: " + error.getMessage()));
    }
}
