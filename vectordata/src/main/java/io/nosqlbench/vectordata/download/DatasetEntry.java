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

// TODO: default download location should use one of the established from nb, jvector, hugging
//  face, but should warn if there are multiple


import io.nosqlbench.vectordata.download.chunker.ChunkedDownloader;

import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public record DatasetEntry(
    String name,
    URL url,
    Map<String, String> attributes,
    Map<String, Map<String, Object>> datasets,
    Map<String, String> tokens,
    Map<String, String> tags
) {
    public DownloadProgress download(Path target) {
        return download(target, false);
    }

    public DownloadProgress download(Path target, boolean force) {
        ChunkedDownloader downloader = new ChunkedDownloader(url, name, 1024 * 1024 *10, 1,
            new StdoutDownloadEventSink());
        return downloader.download(target, force);
    }
}
