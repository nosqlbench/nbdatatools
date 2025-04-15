package io.nosqlbench.vectordata.download.merkle;

import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/// Builder for configuring and creating Merkle tree-based downloads.
///
/// This builder provides a fluent API for configuring all aspects of a
/// Merkle tree-based download operation, including span mode, retry behavior,
/// callbacks, and chunking options.
///
public class MerkleDownloadBuilder {
    private SpanMode spanMode = SpanMode.FULL_SPAN;
    private long targetSpanSize;
    private URL sourceUrl;
    private URL merkleUrl;
    private Path targetPath;
    private int retryCount = 3;
    private Duration timeout = Duration.ofMinutes(5);
    private Duration retryDelay = Duration.ofSeconds(1);
    private Consumer<ReconciliationProgress> progressCallback;
    private Consumer<ReconciliationComplete> completionCallback;
    private boolean useChunkedDownloader = false;
    private int chunkSize = 1024 * 1024 * 16; // 16MB default
    private int maxConcurrentChunks = 4;

    /// Creates a new builder for the specified source URL.
    ///
    /// @param sourceUrl the URL of the file to download
    /// @return a new builder instance
    public static MerkleDownloadBuilder forUrl(URL sourceUrl) {
        return new MerkleDownloadBuilder().sourceUrl(sourceUrl);
    }

    /// Sets the download to use a specific target span size.
    ///
    /// @param spanSize the target span size in bytes
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withTargetSpan(long spanSize) {
        this.spanMode = SpanMode.TARGET_SPAN;
        this.targetSpanSize = spanSize;
        return this;
    }

    /// Sets the download to use the full span of the file.
    ///
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withFullSpan() {
        this.spanMode = SpanMode.FULL_SPAN;
        return this;
    }

    /// Sets the source URL for the download.
    ///
    /// @param url the URL of the file to download
    /// @return this builder for method chaining
    public MerkleDownloadBuilder sourceUrl(URL url) {
        this.sourceUrl = url;
        return this;
    }

    /// Sets the URL for the Merkle tree file.
    ///
    /// @param url the URL of the Merkle tree file
    /// @return this builder for method chaining
    public MerkleDownloadBuilder merkleUrl(URL url) {
        this.merkleUrl = url;
        return this;
    }

    /// Sets the target path where the downloaded file will be saved.
    ///
    /// @param path the path where the file will be saved
    /// @return this builder for method chaining
    public MerkleDownloadBuilder toPath(Path path) {
        this.targetPath = path;
        return this;
    }

    /// Sets the number of retry attempts for failed downloads.
    ///
    /// @param count the number of retry attempts
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withRetries(int count) {
        this.retryCount = count;
        return this;
    }

    /// Sets the timeout for network operations.
    ///
    /// @param timeout the timeout duration
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    /// Sets the delay between retry attempts.
    ///
    /// @param delay the delay duration
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withRetryDelay(Duration delay) {
        this.retryDelay = delay;
        return this;
    }

    /// Sets a callback to be notified of download progress.
    ///
    /// @param callback the progress callback
    /// @return this builder for method chaining
    public MerkleDownloadBuilder onProgress(Consumer<ReconciliationProgress> callback) {
        this.progressCallback = callback;
        return this;
    }

    /// Sets a callback to be notified when the download completes.
    ///
    /// @param callback the completion callback
    /// @return this builder for method chaining
    public MerkleDownloadBuilder onComplete(Consumer<ReconciliationComplete> callback) {
        this.completionCallback = callback;
        return this;
    }

    /// Enables chunked downloading for large files.
    ///
    /// @return this builder for method chaining
    public MerkleDownloadBuilder useChunkedDownloader() {
        this.useChunkedDownloader = true;
        return this;
    }

    /// Sets the size of each chunk when using chunked downloading.
    ///
    /// @param bytes the chunk size in bytes
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withChunkSize(int bytes) {
        this.chunkSize = bytes;
        return this;
    }

    /// Sets the maximum number of concurrent chunks to download.
    ///
    /// @param count the maximum number of concurrent chunks
    /// @return this builder for method chaining
    public MerkleDownloadBuilder withMaxConcurrentChunks(int count) {
        this.maxConcurrentChunks = count;
        return this;
    }

    /// Starts the download operation.
    ///
    /// @return a CompletableFuture that will complete with the download result
    /// @throws IllegalStateException if required parameters are missing
    public CompletableFuture<MerkleDownloadResult> start() {
        validate();
        MerkleDownloadConfig config = new MerkleDownloadConfig(
            sourceUrl,
            merkleUrl,
            targetPath,
            spanMode,
            targetSpanSize,
            retryCount,
            timeout,
            retryDelay,
            progressCallback,
            completionCallback,
            useChunkedDownloader,
            chunkSize,
            maxConcurrentChunks
        );
        return new MerkleDownloadOperation(config).execute();
    }

    /// Validates that all required parameters are set.
    ///
    /// @throws IllegalStateException if required parameters are missing
    private void validate() {
        if (sourceUrl == null) throw new IllegalStateException("Source URL is required");
        if (targetPath == null) throw new IllegalStateException("Target path is required");
        if (merkleUrl == null) {
            merkleUrl = deriveMerkleUrl(sourceUrl);
        }
    }

    /// Derives the Merkle tree URL from the source URL by appending ".merkle".
    ///
    /// @param sourceUrl the source file URL
    /// @return the derived Merkle tree URL
    /// @throws IllegalStateException if the URL cannot be derived
    private URL deriveMerkleUrl(URL sourceUrl) {
        try {
            return new URL(sourceUrl.toString() + ".merkle");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to derive merkle URL", e);
        }
    }

    // Getter methods for the internal MerkleDownloadOperation class
    URL getSourceUrl() { return sourceUrl; }
    URL getMerkleUrl() { return merkleUrl; }
    public Path getTargetPath() { return targetPath; }
    public long getTargetSpanSize() { return targetSpanSize; }
    int getRetryCount() { return retryCount; }
    public Duration getTimeout() { return timeout; }
    Duration getRetryDelay() { return retryDelay; }
    Consumer<ReconciliationProgress> getProgressCallback() { return progressCallback; }
    Consumer<ReconciliationComplete> getCompletionCallback() { return completionCallback; }
    public boolean isUseChunkedDownloader() { return useChunkedDownloader; }
    int getChunkSize() { return chunkSize; }
    public int getMaxConcurrentChunks() { return maxConcurrentChunks; }
}
