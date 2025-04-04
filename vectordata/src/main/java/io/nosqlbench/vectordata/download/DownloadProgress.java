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


import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public record DownloadProgress(
    Path targetPath,
    long totalBytes,
    AtomicLong currentBytes,
    CompletableFuture<DownloadResult> future
) {
    public boolean isDone() {
        return future.isDone();
    }

    public double getProgress() {
        return totalBytes > 0 ? (double) currentBytes.get() / totalBytes : 0.0;
    }

    public DownloadResult get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    public DownloadResult get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    /**
     * Polls for the download result, blocking for the specified timeout.
     *
     * @param timeout The maximum time to wait.
     * @param unit    The time unit of the timeout argument.
     * @return The DownloadResult if it becomes available within the timeout, or null if the timeout expires.
     * @throws InterruptedException If the current thread is interrupted while waiting.
     */
    public DownloadResult poll(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException | ExecutionException e) {
            return null;
        }
    }
}
