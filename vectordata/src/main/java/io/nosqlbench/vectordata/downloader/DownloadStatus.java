package io.nosqlbench.vectordata.downloader;

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


/// Represents the status of a dataset download operation.
///
/// This enum defines the possible states of a download operation:
/// - DOWNLOADED: The file was successfully downloaded
/// - SKIPPED: The file already existed and was not downloaded again
/// - FAILED: The download failed with an error
public enum DownloadStatus {
    /// File was successfully downloaded
    DOWNLOADED,
    /// File existed and was correct size
    SKIPPED,
    /// Download failed with an error
    FAILED
}
