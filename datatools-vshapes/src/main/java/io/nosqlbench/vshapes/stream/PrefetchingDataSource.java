package io.nosqlbench.vshapes.stream;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/// A wrapper DataSource that prefetches chunks in a background thread.
///
/// ## Double-Buffering
///
/// This implementation uses double-buffering to overlap I/O with processing:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                        DOUBLE-BUFFERING TIMELINE                        │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Time ────────────────────────────────────────────────────────────────────►
///
///   I/O Thread:   [Load Chunk 1][Load Chunk 2][Load Chunk 3][Load Chunk 4]...
///                       │            │            │            │
///                       ▼            ▼            ▼            ▼
///   Buffer:           ┌───┐       ┌───┐       ┌───┐       ┌───┐
///                     │ 1 │       │ 2 │       │ 3 │       │ 4 │
///                     └───┘       └───┘       └───┘       └───┘
///                       │            │            │            │
///                       ▼            ▼            ▼            ▼
///   Process Thread:        [Process 1][Process 2][Process 3][Process 4]
///
///   Without prefetching:  Load → Process → Load → Process → Load → Process
///   With prefetching:     Load ──────────────────────────────────────────►
///                              Process ─────────────────────────────────►
/// ```
///
/// ## Buffer Size
///
/// The prefetch buffer holds up to 2 chunks by default (configurable):
/// - One chunk being processed
/// - One chunk prefetched and ready
///
/// This provides overlap without excessive memory usage.
///
/// ## Usage
///
/// ```java
/// // Wrap any DataSource with prefetching
/// DataSource base = TransposedChunkDataSource.builder()
///     .file(path)
///     .build();
///
/// PrefetchingDataSource prefetching = new PrefetchingDataSource(base);
///
/// // Use normally - prefetching happens automatically
/// for (float[][] chunk : prefetching.chunks(10000)) {
///     processChunk(chunk);
/// }
///
/// // Or with AnalyzerHarness
/// harness.run(prefetching, chunkSize);
/// ```
///
/// ## Memory Pressure Handling
///
/// When configured with a [MemoryPressureMonitor], this source adapts its
/// prefetching behavior based on JVM heap usage:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                      MEMORY-AWARE PREFETCHING                           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
///   Memory Pressure   │  Behavior
///   ──────────────────┼──────────────────────────────────────────────────────
///   LOW (< 70%)       │  Full prefetch (configured buffer size)
///   MODERATE (70-85%) │  Reduced prefetch (half buffer size, min 1)
///   HIGH (> 85%)      │  Pause prefetch, wait for GC
/// ```
///
/// ## Thread Safety
///
/// This class is thread-safe for single-consumer iteration. The prefetch
/// thread runs independently and communicates via a blocking queue.
///
/// @see DataSource
/// @see TransposedChunkDataSource
/// @see MemoryPressureMonitor
public final class PrefetchingDataSource implements DataSource {

    private static final Logger logger = LogManager.getLogger(PrefetchingDataSource.class);

    /// Default number of chunks to prefetch (buffer size).
    public static final int DEFAULT_PREFETCH_COUNT = 2;

    /// Default wait time for memory relief in milliseconds.
    private static final long DEFAULT_MEMORY_WAIT_MS = 5000;

    private final DataSource delegate;
    private final int prefetchCount;
    private final MemoryPressureMonitor memoryMonitor;

    /// Creates a prefetching wrapper with default buffer size (2 chunks).
    ///
    /// @param delegate the underlying data source to wrap
    public PrefetchingDataSource(DataSource delegate) {
        this(delegate, DEFAULT_PREFETCH_COUNT, null);
    }

    /// Creates a prefetching wrapper with custom buffer size.
    ///
    /// @param delegate the underlying data source to wrap
    /// @param prefetchCount number of chunks to buffer (minimum 1)
    public PrefetchingDataSource(DataSource delegate, int prefetchCount) {
        this(delegate, prefetchCount, null);
    }

    /// Creates a prefetching wrapper with custom buffer size and memory monitoring.
    ///
    /// When a memory monitor is provided, prefetching adapts to memory pressure:
    /// - HIGH pressure: pauses prefetching and waits for GC
    /// - MODERATE pressure: reduces effective buffer size
    /// - LOW pressure: uses full configured buffer size
    ///
    /// @param delegate the underlying data source to wrap
    /// @param prefetchCount number of chunks to buffer (minimum 1)
    /// @param memoryMonitor optional memory pressure monitor (may be null)
    public PrefetchingDataSource(DataSource delegate, int prefetchCount, MemoryPressureMonitor memoryMonitor) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate cannot be null");
        }
        if (prefetchCount < 1) {
            throw new IllegalArgumentException("prefetchCount must be at least 1, got: " + prefetchCount);
        }
        this.delegate = delegate;
        this.prefetchCount = prefetchCount;
        this.memoryMonitor = memoryMonitor;
    }

    @Override
    public DataspaceShape getShape() {
        return delegate.getShape();
    }

    @Override
    public Iterable<float[][]> chunks(int chunkSize) {
        return () -> new PrefetchingIterator(delegate.chunks(chunkSize).iterator(), prefetchCount, memoryMonitor);
    }

    /// Returns the memory pressure monitor, if configured.
    ///
    /// @return the memory monitor, or null if not configured
    public MemoryPressureMonitor getMemoryMonitor() {
        return memoryMonitor;
    }

    @Override
    public String getId() {
        return "prefetch:" + delegate.getId();
    }

    /// Returns the underlying data source.
    ///
    /// @return the wrapped data source
    public DataSource getDelegate() {
        return delegate;
    }

    /// Returns the prefetch buffer size.
    ///
    /// @return number of chunks that can be buffered
    public int getPrefetchCount() {
        return prefetchCount;
    }

    /// Iterator that prefetches chunks in a background thread.
    private static class PrefetchingIterator implements Iterator<float[][]> {

        /// Sentinel value indicating end of stream.
        private static final float[][] END_MARKER = new float[0][0];

        private final Iterator<float[][]> source;
        private final BlockingQueue<float[][]> buffer;
        private final ExecutorService prefetchExecutor;
        private final MemoryPressureMonitor memoryMonitor;
        private final int configuredBufferSize;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        private float[][] nextChunk;
        private boolean started = false;

        PrefetchingIterator(Iterator<float[][]> source, int bufferSize, MemoryPressureMonitor memoryMonitor) {
            this.source = source;
            this.configuredBufferSize = bufferSize;
            this.memoryMonitor = memoryMonitor;

            // Use memory-aware buffer size if monitor is provided
            int effectiveSize = memoryMonitor != null
                ? memoryMonitor.recommendedPrefetchCount(bufferSize)
                : bufferSize;

            this.buffer = new ArrayBlockingQueue<>(Math.max(1, effectiveSize));
            this.prefetchExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "chunk-prefetch");
                t.setDaemon(true);
                return t;
            });

            if (memoryMonitor != null && effectiveSize < bufferSize) {
                logger.debug("Reduced prefetch buffer from {} to {} due to memory pressure",
                    bufferSize, effectiveSize);
            }
        }

        @Override
        public boolean hasNext() {
            ensureStarted();
            if (nextChunk != null) {
                return true;
            }
            try {
                // Wait for next chunk from prefetch thread
                nextChunk = buffer.take();
                if (nextChunk == END_MARKER) {
                    nextChunk = null;
                    shutdown();
                    // Check for error
                    Throwable t = error.get();
                    if (t != null) {
                        throw new RuntimeException("Error during chunk prefetch", t);
                    }
                    return false;
                }
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                shutdown();
                return false;
            }
        }

        @Override
        public float[][] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            float[][] result = nextChunk;
            nextChunk = null;
            return result;
        }

        private void ensureStarted() {
            if (!started) {
                started = true;
                prefetchExecutor.submit(this::prefetchLoop);
            }
        }

        private void prefetchLoop() {
            try {
                while (source.hasNext() && !done.get()) {
                    // Check memory pressure before loading next chunk
                    if (memoryMonitor != null && memoryMonitor.shouldPausePrefetch()) {
                        logger.debug("Pausing prefetch due to high memory pressure");
                        boolean relieved = memoryMonitor.waitForMemoryRelief(DEFAULT_MEMORY_WAIT_MS);
                        if (!relieved && memoryMonitor.shouldPausePrefetch()) {
                            // Still under pressure but continue anyway to avoid deadlock
                            logger.warn("Continuing prefetch despite memory pressure");
                        }
                    }

                    float[][] chunk = source.next();
                    // Block if buffer is full (backpressure)
                    buffer.put(chunk);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug("Prefetch thread interrupted");
            } catch (Throwable t) {
                error.set(t);
                logger.error("Error in prefetch thread", t);
            } finally {
                try {
                    buffer.put(END_MARKER);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void shutdown() {
            done.set(true);
            prefetchExecutor.shutdown();
            try {
                if (!prefetchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    prefetchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                prefetchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Creates a builder for PrefetchingDataSource.
    ///
    /// @return a new builder
    public static Builder builder() {
        return new Builder();
    }

    /// Wraps a DataSource with prefetching using default settings.
    ///
    /// @param source the source to wrap
    /// @return a prefetching wrapper
    public static PrefetchingDataSource wrap(DataSource source) {
        return new PrefetchingDataSource(source);
    }

    /// Builder for PrefetchingDataSource.
    public static final class Builder {
        private DataSource delegate;
        private int prefetchCount = DEFAULT_PREFETCH_COUNT;
        private MemoryPressureMonitor memoryMonitor;

        private Builder() {}

        /// Sets the underlying data source to wrap.
        ///
        /// @param delegate the data source
        /// @return this builder
        public Builder source(DataSource delegate) {
            this.delegate = delegate;
            return this;
        }

        /// Sets the prefetch buffer size.
        ///
        /// @param count number of chunks to buffer
        /// @return this builder
        public Builder prefetchCount(int count) {
            this.prefetchCount = count;
            return this;
        }

        /// Enables memory pressure monitoring with default thresholds.
        ///
        /// When enabled, prefetching adapts to heap memory usage:
        /// - Pauses when heap usage exceeds 85%
        /// - Reduces buffer when heap usage exceeds 70%
        ///
        /// @return this builder
        public Builder withMemoryMonitoring() {
            this.memoryMonitor = new MemoryPressureMonitor();
            return this;
        }

        /// Sets a custom memory pressure monitor.
        ///
        /// @param monitor the memory pressure monitor
        /// @return this builder
        public Builder memoryMonitor(MemoryPressureMonitor monitor) {
            this.memoryMonitor = monitor;
            return this;
        }

        /// Builds the PrefetchingDataSource.
        ///
        /// @return the configured prefetching data source
        /// @throws IllegalStateException if delegate is not set
        public PrefetchingDataSource build() {
            if (delegate == null) {
                throw new IllegalStateException("delegate must be set");
            }
            return new PrefetchingDataSource(delegate, prefetchCount, memoryMonitor);
        }
    }
}

