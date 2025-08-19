package io.nosqlbench.vectordata.merklev2;

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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicator;
import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture;
import io.nosqlbench.vectordata.downloader.ProgressTrackingTransportClient;
import io.nosqlbench.vectordata.events.EventSink;
import io.nosqlbench.vectordata.events.NoOpEventSink;
import io.nosqlbench.vectordata.merklev2.schedulers.DefaultChunkScheduler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

///
/// A virtualized AsynchronousFileChannel that provides transparent access to remote content
/// with local caching and Merkle tree integrity verification.
///
/// This implementation:
/// 1. Virtualizes file size to match the remote content size
/// 2. Downloads and caches content chunks on demand
/// 3. Uses MerkleState to track verified chunks
/// 4. Provides transparent read access to verified content
///
/// ## Initialization Logic
/// When a MAFileChannel is created, the following preconditions and actions are performed:
/// 1. If there is no cache file and no mrkl state file and no mref reference file, then the
/// remote mref file is downloaded and then a mrkl state file is created from it, then the mref
/// reference file is discarded. Then the MAFileChannel is created with this state file and a new
///  cache content file channel.
/// 2. If there is a cache file and a mrkl state file, then they are both loaded as-is with no
/// modification into a new MAFileChannel.
/// 3. Any other initial state is considered invalid and an error should be thrown from the
/// constructor.
///
/// ## During Reads
/// When an MAFileChannel is read, each read operation has an initial precondition that the
/// matching mrkl state nodes must be valid. When there are matching leaf nodes which are not
/// valid, the content is downloaded and verified, and then the matching leaf node is marked valid.
/// This must all occur before the read to the underlying cache file is performed.
/// When the nodes are already marked as valid, this signals that the underlying cache file is
/// already valid to be read directly with no change to the affected chunks.
///
/// ## Merkle State Semantics
/// When a MerkleState is used with an MAFileChannel, the following semantics are enforced:
/// * The hash values for the merkle tree are never changed. Only the valid bits are changed.
/// * Bits can only be set to true. They cannot be set to false except during initialization.
/// * When a bit is set to true which is not already true, if it has a sibling and that sibling
/// is also set to true, then the parent bit is set to true before the call to change the bit
/// state returns. This should be non-blocking, as it should be an idempotent operation with
/// other overlapping bit changes.
///
/// ## Scheduling
/// When remote data needs to be read, the way it is scheduled to be fetched via the chunked
/// transport client is controlled by a separate scheduler implementation. The scheduler gets a
/// vew of the current MerkleShape, and the MerkleState. When the scheduler is called, it is
/// given the byte range of data that is meant to be read, and it returns a list of download
/// tasks. These tasks are then executed concurrently with the help of the chunked transport client.
/// A barrier condition joins the completion of the tasks together. Each task uses the available
/// retries independently. The schedule also takes care to ensure that the same chunk is not
/// enqueued more than once. Thus if multiple callers try to read from the same chunk
/// concurrently, only one download task should be generated and both callers should ultimately
/// block on the same returned future.
public class MAFileChannel extends AsynchronousFileChannel {
    private final MerkleState merkleState;
    private final AsynchronousFileChannel localContentCache;
    private final ChunkedTransportClient transport;
    private final MerkleShape merkleShape;
    private volatile ChunkScheduler chunkScheduler;
    private final OptimizedChunkQueue chunkQueue;
    private final TaskExecutor taskExecutor;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final MeterRegistry meterRegistry;
    private final EventSink eventSink;
    
    private static final int DEFAULT_MAX_CONCURRENT_DOWNLOADS = 8;
    private static final int DEFAULT_TASK_QUEUE_CAPACITY = 1000;

    /**
     * Creates a new MAFileChannel according to the initialization logic specified in the class documentation.
     * 
     * @param localCachePath Path to local cache file
     * @param merkleStatePath Path to merkle state file  
     * @param remoteSource Remote source URL
     * @throws IOException If initialization fails or invalid state combination is encountered
     */
    public MAFileChannel(Path localCachePath, Path merkleStatePath, String remoteSource) throws IOException {
        this(localCachePath, merkleStatePath, remoteSource, new DefaultChunkScheduler());
    }
    
    /**
     * Creates a new MAFileChannel with a custom scheduler.
     * 
     * @param localCachePath Path to local cache file
     * @param merkleStatePath Path to merkle state file  
     * @param remoteSource Remote source URL
     * @param chunkScheduler Custom chunk scheduler implementation
     * @throws IOException If initialization fails or invalid state combination is encountered
     */
    public MAFileChannel(Path localCachePath, Path merkleStatePath, String remoteSource, ChunkScheduler chunkScheduler) throws IOException {
        // Ensure merkleStatePath ends with .mrkl (state file, not reference file)
        String statePathStr = merkleStatePath.toString();
        Path mrklStatePath;
        
        if (statePathStr.endsWith(".mrkl")) {
            mrklStatePath = merkleStatePath;
        } else if (statePathStr.endsWith(".mref")) {
            // If someone incorrectly passed a .mref path, convert it to .mrkl
            String baseName = statePathStr.substring(0, statePathStr.length() - 5); // Remove .mref
            mrklStatePath = merkleStatePath.resolveSibling(merkleStatePath.getFileName().toString().replace(".mref", ".mrkl"));
        } else {
            // For any other extension, append .mrkl
            mrklStatePath = merkleStatePath.resolveSibling(merkleStatePath.getFileName() + ".mrkl");
        }
            
        // Check current state to determine initialization path
        boolean cacheExists = java.nio.file.Files.exists(localCachePath);
        boolean mrklExists = java.nio.file.Files.exists(mrklStatePath);
        
        // Determine initialization path based on existing files
        MerkleState state;
        AsynchronousFileChannel cache;
        
        if (!cacheExists && !mrklExists) {
            // Case 1: No cache file and no mrkl state file
            // Download remote mref file, create mrkl state file from it, discard mref
            String refUrl = remoteSource + ".mref";
            Path tempRefPath = mrklStatePath.resolveSibling(mrklStatePath.getFileName() + ".tmp.mref");
            
            MerkleRef ref = downloadRef(refUrl, tempRefPath);
            state = MerkleState.fromRef(ref, mrklStatePath);
            
            // Clean up temp ref file
            java.nio.file.Files.deleteIfExists(tempRefPath);
            
            // Create new cache file
            cache = AsynchronousFileChannel.open(localCachePath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                
        } else if (cacheExists && mrklExists) {
            // Case 2: Both cache file and mrkl state file exist
            // Load both as-is with no modification
            state = MerkleState.load(mrklStatePath);
            cache = AsynchronousFileChannel.open(localCachePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
                
        } else {
            // Case 3: Any other initial state is invalid
            String errorMsg = String.format(
                "Invalid initialization state: cache file %s (exists=%b), mrkl state file %s (exists=%b). " +
                "Either both files must exist or neither must exist.",
                localCachePath, cacheExists, mrklStatePath, mrklExists);
            throw new IOException(errorMsg);
        }
        
        // Create base transport client
        ChunkedTransportClient baseTransport = ChunkedTransportIO.create(remoteSource);
        
        // Initialize fields first to access merkle shape
        this.merkleState = state;
        this.localContentCache = cache;
        this.merkleShape = state.getMerkleShape();
        this.chunkScheduler = chunkScheduler;
        
        // Initialize metrics registry and event sink
        this.meterRegistry = Metrics.globalRegistry;
        this.eventSink = NoOpEventSink.INSTANCE;
        
        // Create progress tracking transport client
        int totalChunks = merkleShape.getTotalChunks();
        long totalSize = merkleShape.getTotalContentSize();
        this.transport = new ProgressTrackingTransportClient(baseTransport, totalSize, totalChunks, eventSink);
        
        // Initialize task queue and executor
        this.chunkQueue = new OptimizedChunkQueue(DEFAULT_TASK_QUEUE_CAPACITY, meterRegistry);
        this.taskExecutor = new TaskExecutor(
            chunkQueue, transport, merkleState, merkleShape, localContentCache,
            DEFAULT_MAX_CONCURRENT_DOWNLOADS
        );
        
        // Initialize metrics
        initializeMetrics();
    }
    
    /// Initializes Micrometer metrics for MAFileChannel performance monitoring.
    private void initializeMetrics() {
        // Timer for read operations
        Timer.builder("ma_file_channel.read.duration")
            .description("Time spent in read operations")
            .register(meterRegistry);
            
        // Counter for read operations
        Counter.builder("ma_file_channel.read.operations")
            .description("Total number of read operations")
            .register(meterRegistry);
            
        // Timer for prebuffer operations
        Timer.builder("ma_file_channel.prebuffer.duration")
            .description("Time spent in prebuffer operations")
            .register(meterRegistry);
            
        // Distribution summary for read sizes
        DistributionSummary.builder("ma_file_channel.read.bytes")
            .description("Distribution of bytes read per operation")
            .register(meterRegistry);
            
        // Gauge for file size
        Gauge.builder("ma_file_channel.file.size", this, fc -> {
                try {
                    return fc.size();
                } catch (IOException e) {
                    return -1.0;
                }
            })
            .description("Total file size in bytes")
            .register(meterRegistry);
            
        // Gauge for in-flight downloads
        Gauge.builder("ma_file_channel.downloads.in_flight", this, MAFileChannel::getInFlightDownloadCount)
            .description("Number of downloads currently in flight")
            .register(meterRegistry);
    }

    @Override
    public long size() throws IOException {
        return merkleShape.getTotalContentSize();
    }

    @Override
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (!open.get()) {
            handler.failed(new IOException("Channel is closed"), attachment);
            return;
        }

        CompletableFuture.supplyAsync(() -> {
            try {
                return readSync(dst, position);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                handler.failed(throwable instanceof IOException ? (IOException) throwable : new IOException(throwable), attachment);
            } else {
                handler.completed(result, attachment);
            }
        });
    }

    @Override
    public Future<Integer> read(ByteBuffer dst, long position) {
        if (!open.get()) {
            return CompletableFuture.failedFuture(new IOException("Channel is closed"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                return readSync(dst, position);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int readSync(ByteBuffer dst, long position) throws IOException {
        Timer.Sample sample = Timer.start(meterRegistry);
        meterRegistry.counter("ma_file_channel.read.operations").increment();
        
        try {
            if (!open.get()) throw new IOException("Channel is closed");
            
            int totalBytesRead = 0;
            long currentPosition = position;
            int requestedLength = dst.remaining();
            
            // Use new node-centric scheduling approach with traceability
            List<SchedulingDecision> decisions = chunkScheduler.analyzeSchedulingDecisions(position, (long) requestedLength, merkleShape, merkleState);
            
            // Execute the scheduling decisions using the optimized chunk queue
            OptimizedChunkQueue.SchedulingResult schedulingResult = chunkQueue.executeSchedulingWithTasks(wrapper -> {
                for (SchedulingDecision decision : decisions) {
                    int nodeIndex = decision.nodeIndex();
                    MerkleShape.MerkleNodeRange byteRange = merkleShape.getByteRangeForNode(nodeIndex);
                    MerkleShape.MerkleNodeRange leafRange = merkleShape.getLeafRangeForNode(nodeIndex);
                    
                    // Create a download task based on the scheduling decision
                    ChunkScheduler.NodeDownloadTask task = new DecisionBasedDownloadTask(
                        nodeIndex,
                        byteRange.getStart(),
                        byteRange.getLength(),
                        merkleShape.isLeafNode(nodeIndex),
                        leafRange,
                        wrapper.getOrCreateFuture(nodeIndex)
                    );
                    
                    wrapper.offerTask(task);
                }
            });
            
            // Filter futures to only wait for those relevant to this read region
            List<CompletableFuture<Void>> relevantFutures = filterRelevantFuturesFromTasks(
                schedulingResult.tasks(), position, requestedLength
            );
            
            // Wait only for relevant downloads to complete
            if (!relevantFutures.isEmpty()) {
                CompletableFuture<Void> allComplete = CompletableFuture.allOf(
                    relevantFutures.toArray(new CompletableFuture[0])
                );
                allComplete.join(); // This will throw if any download fails
            }
            
            // Now all required chunks should be available - read from local cache
            while (dst.hasRemaining() && currentPosition < size()) {
                int chunkIndex = merkleShape.getChunkIndexForPosition(currentPosition);
                
                // Verify chunk is now available (should be true after downloads)
                if (!merkleState.isValid(chunkIndex)) {
                    throw new IOException("Chunk " + chunkIndex + " is still not valid after download");
                }
                
                // Calculate read parameters for this chunk
                long chunkStart = merkleShape.getChunkStartPosition(chunkIndex);
                long chunkEnd = merkleShape.getChunkEndPosition(chunkIndex);
                long readStart = Math.max(currentPosition, chunkStart);
                long readEnd = Math.min(currentPosition + dst.remaining(), chunkEnd);
                int readLength = (int) (readEnd - readStart);
                
                if (readLength <= 0) break;
                
                // Read from local cache
                ByteBuffer chunkBuffer = dst.slice();
                chunkBuffer.limit(readLength);
                
                Future<Integer> readFuture = localContentCache.read(chunkBuffer, readStart);
                int bytesRead = readFuture.get();
                
                if (bytesRead <= 0) break;
                
                dst.position(dst.position() + bytesRead);
                totalBytesRead += bytesRead;
                currentPosition += bytesRead;
            }
            
            // Record metrics
            meterRegistry.summary("ma_file_channel.read.bytes").record(totalBytesRead);
            
            return totalBytesRead;
        } catch (Exception e) {
            meterRegistry.counter("ma_file_channel.read.operations", "status", "failed").increment();
            throw new IOException("Read operation failed", e);
        } finally {
            sample.stop(Timer.builder("ma_file_channel.read.duration")
                .register(meterRegistry));
        }
    }


    // Unsupported operations for read-only virtual file
    @Override
    public AsynchronousFileChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Truncate not supported on virtual file");
    }

    @Override
    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        handler.failed(new UnsupportedOperationException("Write not supported on virtual file"), attachment);
    }

    @Override
    public Future<Integer> write(ByteBuffer src, long position) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Write not supported on virtual file"));
    }

    @Override
    public void force(boolean metaData) throws IOException {
        if (open.get()) {
            localContentCache.force(metaData);
            merkleState.flush();
        }
    }

    @Override
    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {
        localContentCache.lock(position, size, shared, attachment, handler);
    }

    @Override
    public Future<FileLock> lock(long position, long size, boolean shared) {
        return localContentCache.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return localContentCache.tryLock(position, size, shared);
    }

    /**
     * Asynchronously prebuffers a range of bytes by ensuring all chunks containing the range are downloaded.
     * This method does not block and returns immediately with a CompletableFuture that completes when
     * all chunks in the range are available. The returned future also implements ProgressTrackingCompletableFuture
     * for callers that want to track download progress.
     *
     * @param position The starting position in the file
     * @param length The number of bytes to prebuffer
     * @return A CompletableFuture that completes when all chunks in the range are available
     */
    public CompletableFuture<Void> prebuffer(long position, long length) {
        Timer.Sample sample = Timer.start(meterRegistry);
        meterRegistry.counter("ma_file_channel.prebuffer.operations").increment();
        
        if (position < 0) {
            meterRegistry.counter("ma_file_channel.prebuffer.operations", "status", "failed").increment();
            ProgressIndicatingFuture<Void> errorFuture = new ProgressIndicatingFuture<>(
                CompletableFuture.<Void>failedFuture(new IOException("Negative position: " + position)),
                () -> 0.0, () -> 0.0, (double)merkleShape.getChunkSize());
            return errorFuture;
        }
        if (length < 0) {
            meterRegistry.counter("ma_file_channel.prebuffer.operations", "status", "failed").increment();
            ProgressIndicatingFuture<Void> errorFuture = new ProgressIndicatingFuture<>(
                CompletableFuture.<Void>failedFuture(new IOException("Negative length: " + length)),
                () -> 0.0, () -> 0.0, (double)merkleShape.getChunkSize());
            return errorFuture;
        }

        try {
            // Calculate total work (chunks that need to be downloaded)
            final int startChunk = merkleShape.getChunkIndexForPosition(position);
            final int endChunk = merkleShape.getChunkIndexForPosition(
                Math.min(position + length - 1, merkleShape.getTotalContentSize() - 1));
            
            // Count initially valid chunks (already downloaded)
            int initiallyValidChunks = 0;
            for (int chunk = startChunk; chunk <= endChunk; chunk++) {
                if (merkleState.isValid(chunk)) {
                    initiallyValidChunks++;
                }
            }
            
            // Create atomic counter for progress tracking that won't block on file I/O
            final java.util.concurrent.atomic.AtomicInteger completedChunks = 
                new java.util.concurrent.atomic.AtomicInteger(initiallyValidChunks);
            final int totalChunks = endChunk - startChunk + 1;
            
            // Use iterative scheduling to handle large requests that may exceed transport limits
            CompletableFuture<Void> downloadFuture = executeScheduledDownloadsIteratively(position, length, completedChunks, startChunk, endChunk);
            
            // Create progress tracking future with fast, non-blocking callbacks
            ProgressIndicatingFuture<Void> progressFuture = new ProgressIndicatingFuture<>(
                downloadFuture,
                // Total work callback - simply return the calculated total
                () -> (double)totalChunks,
                // Current work callback - use atomic counter (no file I/O blocking)
                () -> (double)completedChunks.get(),
                // Bytes per unit - use chunk size for contextual byte display
                (double)merkleShape.getChunkSize()
            );
            
            // Return future with proper cleanup
            progressFuture.whenComplete((v, throwable) -> {
                // Record metrics
                sample.stop(Timer.builder("ma_file_channel.prebuffer.duration")
                    .register(meterRegistry));
                
                if (throwable != null) {
                    meterRegistry.counter("ma_file_channel.prebuffer.operations", "status", "failed").increment();
                } else {
                    meterRegistry.counter("ma_file_channel.prebuffer.operations", "status", "success").increment();
                }
            });
            
            return progressFuture;
            
        } catch (Exception e) {
            sample.stop(Timer.builder("ma_file_channel.prebuffer.duration")
                .register(meterRegistry));
            meterRegistry.counter("ma_file_channel.prebuffer.operations", "status", "failed").increment();
            ProgressIndicatingFuture<Void> errorFuture = new ProgressIndicatingFuture<>(
                CompletableFuture.<Void>failedFuture(e),
                () -> 0.0, () -> 0.0, (double)merkleShape.getChunkSize());
            return errorFuture;
        }
    }

    /// Swaps the current scheduler with a new one.
    /// 
    /// The scheduler is stateless by design, so it can be safely swapped at runtime
    /// without affecting ongoing operations. The new scheduler will be used for all
    /// subsequent read requests, while existing in-flight downloads will continue
    /// using their original scheduling decisions.
    /// 
    /// @param newScheduler The new scheduler to use
    /// @throws IllegalArgumentException if newScheduler is null
    public void setChunkScheduler(ChunkScheduler newScheduler) {
        if (newScheduler == null) {
            throw new IllegalArgumentException("Scheduler cannot be null");
        }
        this.chunkScheduler = newScheduler;
    }
    
    /// Gets the current scheduler.
    /// 
    /// @return The current chunk scheduler
    public ChunkScheduler getChunkScheduler() {
        return chunkScheduler;
    }
    
    /// Filters download task futures to only include those that overlap with the specified read region.
    /// 
    /// This allows callers to wait only for the downloads that are actually needed for their
    /// specific read operation, rather than blocking on all scheduled downloads.
    /// 
    /// @param taskFutures All task futures from scheduling
    /// @param readPosition Starting position of the read request
    /// @param readLength Number of bytes to read
    /// @return List of futures that overlap with the read region
    private List<CompletableFuture<Void>> filterRelevantFutures(
            List<CompletableFuture<Void>> taskFutures, long readPosition, int readLength) {
        
        // If no tasks were scheduled, return empty list
        if (taskFutures.isEmpty()) {
            return taskFutures;
        }
        
        List<CompletableFuture<Void>> relevantFutures = new ArrayList<>();
        long readEndPosition = readPosition + readLength;
        
        // We need to find which in-flight downloads correspond to these futures
        // and check if they overlap with our read region
        for (CompletableFuture<Void> future : taskFutures) {
            if (futureOverlapsReadRegion(future, readPosition, readEndPosition)) {
                relevantFutures.add(future);
            }
        }
        
        return relevantFutures;
    }
    
    /// Determines if a download future overlaps with the specified read region.
    /// 
    /// This method checks the in-flight downloads to find the node associated with
    /// the given future and determines if its byte range overlaps with the read region.
    /// 
    /// @param future The download future to check
    /// @param readStart Starting position of the read request
    /// @param readEnd Ending position of the read request (exclusive)
    /// @return true if the future's download overlaps with the read region
    private boolean futureOverlapsReadRegion(CompletableFuture<Void> future, long readStart, long readEnd) {
        // Find the node index associated with this future
        for (var entry : chunkQueue.inFlightFutures().entrySet()) {
            if (entry.getValue() == future) {
                int nodeIndex = entry.getKey();
                
                // Get the byte range covered by this node
                MerkleShape.MerkleNodeRange nodeRange = merkleShape.getByteRangeForNode(nodeIndex);
                long nodeStart = nodeRange.getStart();
                long nodeEnd = nodeStart + nodeRange.getLength();
                
                // Check if there's any overlap between node range and read range
                return nodeStart < readEnd && nodeEnd > readStart;
            }
        }
        
        // If we can't find the future in the map, conservatively assume it overlaps
        // This can happen if the future completed and was removed between scheduling and filtering
        return true;
    }
    
    /// Filters futures based on tasks that cover chunks required for the read operation.
    /// 
    /// This improved implementation ensures callers only block on futures that are
    /// downloading chunks actually needed for their read, preventing unnecessary blocking
    /// on large internal nodes that may contain unneeded data.
    /// 
    /// @param tasks The download tasks that were scheduled
    /// @param readPosition Starting position of the read request  
    /// @param readLength Number of bytes to read
    /// @return List of futures from tasks that cover required chunks
    private List<CompletableFuture<Void>> filterRelevantFuturesFromTasks(
            List<ChunkScheduler.NodeDownloadTask> tasks, long readPosition, int readLength) {
        
        // Calculate which chunks are actually needed for this read
        int startChunk = merkleShape.getChunkIndexForPosition(readPosition);
        int endChunk = merkleShape.getChunkIndexForPosition(
            Math.min(readPosition + readLength - 1, merkleShape.getTotalContentSize() - 1));
        
        // Create set of required chunks for efficient lookup
        Set<Integer> requiredChunks = new HashSet<>();
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            requiredChunks.add(chunk);
        }
        
        List<CompletableFuture<Void>> relevantFutures = new ArrayList<>();
        
        for (ChunkScheduler.NodeDownloadTask task : tasks) {
            // Check if this task covers any of our required chunks
            MerkleShape.MerkleNodeRange leafRange = task.getLeafRange();
            boolean coversRequiredChunk = false;
            
            // Convert leaf range to actual chunk range, respecting bounds
            int leafStartChunk = (int) leafRange.getStart();
            int leafEndChunk = (int) Math.min(leafRange.getEnd(), merkleShape.getTotalChunks());
            
            // Check intersection with required chunks
            for (int chunkIndex = leafStartChunk; chunkIndex < leafEndChunk; chunkIndex++) {
                if (requiredChunks.contains(chunkIndex)) {
                    coversRequiredChunk = true;
                    break;
                }
            }
            
            if (coversRequiredChunk) {
                relevantFutures.add(task.getFuture());
            }
        }
        
        return relevantFutures;
    }

    /// Gets the number of nodes currently being downloaded.
    /// 
    /// This is useful for monitoring and debugging purposes.
    /// 
    /// @return The number of in-flight downloads
    public int getInFlightDownloadCount() {
        return chunkQueue.getInFlightCount();
    }
    
    
    /// Executes downloads using the chunk scheduler for the specified byte range.
    /// 
    /// This method uses the chunk scheduler to determine optimal nodes to download,
    /// ensuring consistency with the scheduler's strategy (aggressive, conservative, etc).
    /// 
    /// @param position Starting byte position
    /// @param length Number of bytes to cover
    /// @return CompletableFuture that completes when all downloads are finished
    private CompletableFuture<Void> executeScheduledDownloads(long position, long length) {
        // Use the chunk scheduler to analyze and create scheduling decisions
        List<SchedulingDecision> decisions = chunkScheduler.analyzeSchedulingDecisions(
            position, length, merkleShape, merkleState);
        
        // If no downloads needed, return completed future
        if (decisions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        // Execute the scheduling decisions using the optimized chunk queue
        OptimizedChunkQueue.SchedulingResult schedulingResult = chunkQueue.executeSchedulingWithTasks(wrapper -> {
            for (SchedulingDecision decision : decisions) {
                int nodeIndex = decision.nodeIndex();
                MerkleShape.MerkleNodeRange byteRange = merkleShape.getByteRangeForNode(nodeIndex);
                MerkleShape.MerkleNodeRange leafRange = merkleShape.getLeafRangeForNode(nodeIndex);
                
                // Create a download task based on the scheduling decision
                ChunkScheduler.NodeDownloadTask task = new DecisionBasedDownloadTask(
                    nodeIndex,
                    byteRange.getStart(),
                    byteRange.getLength(),
                    merkleShape.isLeafNode(nodeIndex),
                    leafRange,
                    wrapper.getOrCreateFuture(nodeIndex)
                );
                
                wrapper.offerTask(task);
            }
        });
        
        // Return future that waits for all our downloads
        List<CompletableFuture<Void>> allFutures = schedulingResult.tasks().stream()
            .map(ChunkScheduler.NodeDownloadTask::getFuture)
            .collect(java.util.stream.Collectors.toList());
        
        return CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]));
    }

    /// Executes downloads iteratively to handle large requests that may exceed transport limits.
    /// 
    /// This method ensures complete prebuffering of the requested range while respecting
    /// the 2GB limit imposed by ByteBuffer-based transport implementations. It breaks
    /// large requests into manageable chunks and validates complete coverage.
    /// 
    /// @param position Starting byte position
    /// @param length Number of bytes to prebuffer
    /// @param progressCounter Atomic counter for tracking completed chunks (for progress display)
    /// @param startChunk Starting chunk index for progress tracking
    /// @param endChunk Ending chunk index for progress tracking
    /// @return CompletableFuture that completes when the entire range is prebuffered
    private CompletableFuture<Void> executeScheduledDownloadsIteratively(long position, long length, 
            java.util.concurrent.atomic.AtomicInteger progressCounter, int startChunk, int endChunk) {
        // Use 1.5GB as max chunk size to safely stay under 2GB transport limit
        // This leaves room for merkle tree overhead and ensures we don't hit transport limits
        final long MAX_TRANSPORT_CHUNK_SIZE = 1_610_612_736L; // 1.5GB in bytes
        
        // Create a CompletableFuture that we'll complete manually when ALL iterative work is done
        CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        
        // Execute the iterative processing asynchronously
        CompletableFuture.runAsync(() -> {
            try {
                // First, check if any chunks in the range actually need downloading
                // Use the provided startChunk and endChunk parameters
                
                int totalChunksRequired = 0;
                for (int chunk = startChunk; chunk <= endChunk; chunk++) {
                    if (!merkleState.isValid(chunk)) {
                        totalChunksRequired++;
                    }
                }
                
                // If no chunks need downloading, validate the range is complete but maintain consistent timing
                if (totalChunksRequired == 0) {
                    // Still validate that all chunks in the range are truly valid
                    for (int chunk = startChunk; chunk <= endChunk; chunk++) {
                        if (!merkleState.isValid(chunk)) {
                            throw new RuntimeException("Chunk " + chunk + 
                                " is invalid despite being counted as not needing download");
                        }
                    }
                    // Complete after validation - this maintains consistency with download path
                    completionFuture.complete(null);
                    return;
                }
                
                long totalProcessed = 0;
                long currentPosition = position;
                long remainingLength = length;
                int chunksDownloaded = 0;
                
                // Collect all download futures for concurrent execution
                List<CompletableFuture<Void>> allDownloadFutures = new java.util.ArrayList<>();
                
            // Process the range iteratively until complete
            while (remainingLength > 0 && totalProcessed < length) {
                // Calculate next segment size
                long segmentLength = Math.min(remainingLength, MAX_TRANSPORT_CHUNK_SIZE);
                
                // Get required chunks for this segment
                int segmentStartChunk = merkleShape.getChunkIndexForPosition(currentPosition);
                int segmentEndChunk = merkleShape.getChunkIndexForPosition(
                    Math.min(currentPosition + segmentLength - 1, merkleShape.getTotalContentSize() - 1));
                
                // Find chunks that still need downloading in this segment  
                List<Integer> requiredChunks = new java.util.ArrayList<>();
                for (int chunk = segmentStartChunk; chunk <= segmentEndChunk; chunk++) {
                    if (!merkleState.isValid(chunk)) {
                        requiredChunks.add(chunk);
                    }
                }
                
                // If no chunks needed in this segment, move to next
                if (requiredChunks.isEmpty()) {
                    currentPosition += segmentLength;
                    remainingLength -= segmentLength;
                    totalProcessed += segmentLength;
                    continue;
                }
                
                // Use the scheduler to determine optimal nodes for this segment, 
                // with intelligent filtering for transport limits
                List<SchedulingDecision> allDecisions = chunkScheduler.selectOptimalNodes(
                    requiredChunks, merkleShape, merkleState);
                
                // Filter and adapt decisions to respect transport limits
                List<SchedulingDecision> transportCompatibleDecisions = new java.util.ArrayList<>();
                List<Integer> chunksNeedingFallback = new java.util.ArrayList<>();
                
                for (SchedulingDecision decision : allDecisions) {
                    int nodeIndex = decision.nodeIndex();
                    MerkleShape.MerkleNodeRange byteRange = merkleShape.getByteRangeForNode(nodeIndex);
                    
                    if (byteRange.getLength() <= MAX_TRANSPORT_CHUNK_SIZE) {
                        // This node is transport-compatible, use as-is
                        transportCompatibleDecisions.add(decision);
                    } else {
                        // This node is too large, fall back to leaf nodes for its covered chunks
                        for (Integer chunkIndex : decision.coveredChunks()) {
                            if (requiredChunks.contains(chunkIndex) && !merkleState.isValid(chunkIndex)) {
                                chunksNeedingFallback.add(chunkIndex);
                            }
                        }
                    }
                }
                
                // Create leaf node decisions for chunks that need fallback
                for (Integer chunkIndex : chunksNeedingFallback) {
                    int leafNodeIndex = merkleShape.chunkIndexToLeafNode(chunkIndex);
                    MerkleShape.MerkleNodeRange leafByteRange = merkleShape.getByteRangeForNode(leafNodeIndex);
                    
                    // Even leaf nodes might be too large in pathological cases, skip if so
                    if (leafByteRange.getLength() <= MAX_TRANSPORT_CHUNK_SIZE) {
                        SchedulingDecision leafDecision = new SchedulingDecision(
                            leafNodeIndex,
                            SchedulingReason.TRANSPORT_SIZE_FALLBACK,
                            0, // priority
                            leafByteRange.getLength(),
                            List.of(chunkIndex), // required chunks
                            List.of(chunkIndex), // covered chunks (same for leaf nodes)
                            "Fallback to leaf node due to transport size limit (" + 
                                leafByteRange.getLength() + " bytes)"
                        );
                        transportCompatibleDecisions.add(leafDecision);
                    }
                }
                
                // Create tasks from transport-compatible decisions
                OptimizedChunkQueue.SchedulingResult result = chunkQueue.executeSchedulingWithTasks(wrapper -> {
                    for (SchedulingDecision decision : transportCompatibleDecisions) {
                        int nodeIndex = decision.nodeIndex();
                        MerkleShape.MerkleNodeRange byteRange = merkleShape.getByteRangeForNode(nodeIndex);
                        MerkleShape.MerkleNodeRange leafRange = merkleShape.getLeafRangeForNode(nodeIndex);
                        
                        // Create download task for this node
                        ChunkScheduler.NodeDownloadTask task = new DecisionBasedDownloadTask(
                            nodeIndex,
                            byteRange.getStart(),
                            byteRange.getLength(),
                            merkleShape.isLeafNode(nodeIndex),
                            leafRange,
                            wrapper.getOrCreateFuture(nodeIndex)
                        );
                        
                        wrapper.offerTask(task);
                    }
                });
                
                // Collect futures from this segment
                List<CompletableFuture<Void>> currentSegmentFutures = result.tasks().stream()
                    .map(ChunkScheduler.NodeDownloadTask::getFuture)
                    .collect(java.util.stream.Collectors.toList());
                
                // Don't wait for this segment - collect futures for concurrent execution
                // This allows all downloads to run concurrently instead of segment-by-segment
                
                if (requiredChunks.isEmpty()) {
                    // No downloads needed for this segment, continue to next
                } else if (currentSegmentFutures.isEmpty()) {
                    // If we had required chunks but no futures, something went wrong
                    throw new RuntimeException("No download tasks created for required chunks " + 
                        requiredChunks + " at position " + currentPosition + 
                        ". This may indicate all nodes exceed transport limits.");
                } else {
                    // Add futures to collection for concurrent execution and attach progress tracking
                    for (ChunkScheduler.NodeDownloadTask task : result.tasks()) {
                        CompletableFuture<Void> future = task.getFuture();
                        
                        // Determine how many chunks this task covers
                        int chunksForThisTask;
                        if (task.isLeafNode()) {
                            // Leaf nodes always cover exactly 1 chunk
                            chunksForThisTask = 1;
                        } else {
                            // Internal nodes cover multiple chunks - calculate from leaf range
                            MerkleShape.MerkleNodeRange leafRange = task.getLeafRange();
                            chunksForThisTask = (int)(leafRange.getEnd() - leafRange.getStart());
                        }
                        
                        // Attach completion handler for progress tracking
                        CompletableFuture<Void> progressTrackingFuture = future.whenComplete((v, throwable) -> {
                            if (throwable == null) {
                                // Successfully completed - increment progress counter by the number of chunks this task covers
                                progressCounter.addAndGet(chunksForThisTask);
                            }
                        });
                        allDownloadFutures.add(progressTrackingFuture);
                    }
                    chunksDownloaded += requiredChunks.size();
                }
                
                // Move to next segment
                currentPosition += segmentLength;
                remainingLength -= segmentLength;
                totalProcessed += segmentLength;
                
                // Safety check: verify we're making progress
                int currentChunksValid = 0;
                int totalChunksNeeded = merkleShape.getChunkIndexForPosition(
                    Math.min(position + length - 1, merkleShape.getTotalContentSize() - 1)) -
                    merkleShape.getChunkIndexForPosition(position) + 1;
                
                for (int i = 0; i < totalChunksNeeded; i++) {
                    int chunkIndex = merkleShape.getChunkIndexForPosition(position) + i;
                    if (merkleState.isValid(chunkIndex)) {
                        currentChunksValid++;
                    }
                }
                
                // Log progress for large operations
                if (length > MAX_TRANSPORT_CHUNK_SIZE && currentChunksValid > 0) {
                    double progressPercent = (double) currentChunksValid / totalChunksNeeded * 100.0;
                    // This would be logged if logging was available - for now just track progress
                }
            }
            
            // Wait for all concurrent downloads to complete before validation
            if (!allDownloadFutures.isEmpty()) {
                CompletableFuture.allOf(allDownloadFutures.toArray(new CompletableFuture[0])).join();
            }
            
            // Final validation: ensure all required chunks are now valid
            // Use the provided startChunk and endChunk parameters
            
            List<Integer> stillInvalidChunks = new java.util.ArrayList<>();
            for (int chunk = startChunk; chunk <= endChunk; chunk++) {
                if (!merkleState.isValid(chunk)) {
                    stillInvalidChunks.add(chunk);
                }
            }
            
            if (!stillInvalidChunks.isEmpty()) {
                throw new RuntimeException("Iterative prebuffering incomplete. " + 
                    stillInvalidChunks.size() + " chunks remain invalid after processing: " + 
                    stillInvalidChunks + ". This may indicate chunks larger than transport limit (" + 
                    MAX_TRANSPORT_CHUNK_SIZE + " bytes) or scheduler issues.");
            }
            
                // Successfully completed all iterative processing
                completionFuture.complete(null);
                
            } catch (Exception e) {
                // Complete the future exceptionally if anything fails
                completionFuture.completeExceptionally(e);
            }
        });
        
        return completionFuture;
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void close() throws IOException {
        // Use atomic compareAndSet to ensure close happens only once
        if (open.compareAndSet(true, false)) {
            // Close task executor first to stop processing
            taskExecutor.close();
            
            force(false);
            localContentCache.close();
            transport.close();
            merkleState.close();
        }
    }

    /// Implementation of NodeDownloadTask.
    private static class DecisionBasedDownloadTask implements ChunkScheduler.NodeDownloadTask {
        private final int nodeIndex;
        private final long offset;
        private final long size;
        private final boolean isLeaf;
        private final MerkleShape.MerkleNodeRange leafRange;
        private final CompletableFuture<Void> future;
        
        public DecisionBasedDownloadTask(int nodeIndex, long offset, long size, boolean isLeaf,
                                       MerkleShape.MerkleNodeRange leafRange, CompletableFuture<Void> future) {
            this.nodeIndex = nodeIndex;
            this.offset = offset;
            this.size = size;
            this.isLeaf = isLeaf;
            this.leafRange = leafRange;
            this.future = future;
        }
        
        @Override
        public int getNodeIndex() { return nodeIndex; }
        
        @Override
        public long getOffset() { return offset; }
        
        @Override
        public long getSize() { return size; }
        
        @Override
        public boolean isLeafNode() { return isLeaf; }
        
        @Override
        public MerkleShape.MerkleNodeRange getLeafRange() { return leafRange; }
        
        @Override
        public CompletableFuture<Void> getFuture() { return future; }
    }

    public static MerkleRef downloadRef(String refUrl, Path tempPath) throws IOException {
        // Download reference merkle tree
        ChunkedTransportClient client = ChunkedTransportIO.create(refUrl);
        try {
            long size = client.getSize().join();
            FetchResult<?> result = client.fetchRange(0, (int) size).join();
            ByteBuffer refData = result.getData();
            
            // Write to temp file and load as MerkleRef
            byte[] data = new byte[refData.remaining()];
            refData.get(data);
            java.nio.file.Files.createDirectories(tempPath.getParent());
            java.nio.file.Files.write(tempPath, data);
            return MerkleRef.load(tempPath);
        } finally {
            client.close();
        }
    }
}
