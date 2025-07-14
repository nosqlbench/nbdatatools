package io.nosqlbench.vectordata.merkle;

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

import io.nosqlbench.vectordata.downloader.DownloadProgress;
import io.nosqlbench.vectordata.downloader.DownloadResult;
import io.nosqlbench.vectordata.status.EventSink;
import io.nosqlbench.vectordata.status.LogFileEventSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/// An AsynchronousFileChannel implementation that uses MerklePainter to verify and download data as needed.
/// This class acts as a shell around its parent AsynchronousFileChannel, presenting the size from the
/// reference merkle file and ensuring that content is downloaded and verified before any reads.
public class MerkleAsyncFileChannel extends AsynchronousFileChannel {
    private final AsynchronousFileChannel delegate;
    /**
     * Provides access to the underlying AsynchronousFileChannel for direct reads.
     * @return the underlying delegate channel
     */
    public AsynchronousFileChannel getDelegateChannel() {
        return delegate;
    }
    private final MerklePainter painter;

    /// Creates a new MerkleAsyncFileChannel with a local path and source URL.
    ///
    /// @param localPath Path where the local data file exists or will be stored
    /// @param remoteUrl URL of the source data file
    /// @throws IOException If there's an error opening the files or downloading reference data
    public MerkleAsyncFileChannel(Path localPath, String remoteUrl) throws IOException {
        this(localPath, remoteUrl, new LogFileEventSink(localPath.resolveSibling(localPath.getFileName().toString() + ".log")));
    }

    /// Creates a new MerkleAsyncFileChannel with a local path, source URL, and custom event sink.
    ///
    /// @param localPath Path where the local data file exists or will be stored
    /// @param remoteUrl URL of the source data file
    /// @param eventSink Custom event sink for logging events
    /// @throws IOException If there's an error opening the files or downloading reference data
    public MerkleAsyncFileChannel(Path localPath, String remoteUrl, EventSink eventSink) throws IOException {
        // Create the file and parent directories if they don't exist
        Files.createDirectories(localPath.getParent());
        if (!Files.exists(localPath)) {
            Files.createFile(localPath);
        }

        // Open the delegate channel
        this.delegate = AsynchronousFileChannel.open(localPath, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Create a MerklePainter with the given parameters
        this.painter = new MerklePainter(localPath, remoteUrl, eventSink);
    }

    /// Creates a new MerkleAsyncFileChannel specifically for testing purposes.
    /// This constructor delegates to the standard constructor, ignoring the forTesting parameter.
    ///
    /// @param localPath Path where the local data file exists or will be stored
    /// @param remoteUrl URL of the source data file (can be a dummy URL for testing)
    /// @param eventSink Custom event sink for logging events
    /// @param forTesting Flag indicating this is for testing purposes (ignored)
    /// @throws IOException If there's an error opening the files
    public MerkleAsyncFileChannel(Path localPath, String remoteUrl, EventSink eventSink, boolean forTesting) throws IOException {
        this(localPath, remoteUrl, eventSink);
    }

    /// Returns the size of this file as reported by the merkle tree.
    /// This may be different from the actual file size if not all chunks have been downloaded.
    ///
    /// @return The virtual length of the file
    @Override
    public long size() throws IOException {
        return painter.totalSize();
    }

    /// Reads data from this channel into the given buffer, starting at the given file position.
    /// Ensures that the chunks containing the requested data are available before reading.
    ///
    /// @param dst The buffer into which bytes are to be transferred
    /// @param position The file position at which the transfer is to begin
    /// @param attachment The object to attach to the I/O operation
    /// @param handler The handler for consuming the result
    @Override
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        // Ensure the chunks containing the requested data are available
        long endPosition = position + dst.remaining();

        try {
            // Ensure all chunks containing the requested data are available
            painter.paint(position, endPosition);

            // Perform the read operation
            delegate.read(dst, position, attachment, handler);
        } catch (Exception e) {
            handler.failed(e, attachment);
        }
    }

    /// Reads data from this channel into the given buffer, starting at the given file position.
    /// Ensures that the chunks containing the requested data are available before reading.
    ///
    /// @param dst The buffer into which bytes are to be transferred
    /// @param position The file position at which the transfer is to begin
    /// @return A Future representing the pending result of the operation
    @Override
    public Future<Integer> read(ByteBuffer dst, long position) {
        CompletableFuture<Integer> future = new CompletableFuture<>();

        // Ensure the chunks containing the requested data are available
        long endPosition = position + dst.remaining();

        try {
            // Ensure all chunks containing the requested data are available
            painter.paint(position, endPosition);

            // Perform the read operation
            Future<Integer> delegateFuture = delegate.read(dst, position);

            // Set up a completion handler to bridge the Future to our CompletableFuture
            delegateFuture.get(); // This will block until the read is complete

            // If we get here, the read was successful
            int bytesRead = dst.position();
            future.complete(bytesRead);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /// Writes data from the given buffer to this channel, starting at the given file position.
    /// This operation is not supported as the file is read-only from the client perspective.
    ///
    /// @param src The buffer from which bytes are to be transferred
    /// @param position The file position at which the transfer is to begin
    /// @param attachment The object to attach to the I/O operation
    /// @param handler The handler for consuming the result
    @Override
    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        handler.failed(new UnsupportedOperationException("Write operations are not supported"), attachment);
    }

    /// Writes data from the given buffer to this channel, starting at the given file position.
    /// This operation is not supported as the file is read-only from the client perspective.
    ///
    /// @param src The buffer from which bytes are to be transferred
    /// @param position The file position at which the transfer is to begin
    /// @return A Future representing the pending result of the operation
    @Override
    public Future<Integer> write(ByteBuffer src, long position) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Write operations are not supported"));
        return future;
    }

    /// Locks a region of this channel's file.
    /// This operation is not supported as the file is managed by the MerklePainter.
    ///
    /// @param position The position at which the locked region is to start
    /// @param size The size of the locked region
    /// @param shared true if the lock is to be shared, false if it is to be exclusive
    /// @return A Future representing the pending result of the operation
    @Override
    public Future<FileLock> lock(long position, long size, boolean shared) {
        CompletableFuture<FileLock> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Lock operations are not supported"));
        return future;
    }

    /// Locks a region of this channel's file.
    /// This operation is not supported as the file is managed by the MerklePainter.
    ///
    /// @param position The position at which the locked region is to start
    /// @param size The size of the locked region
    /// @param shared true if the lock is to be shared, false if it is to be exclusive
    /// @param attachment The object to attach to the I/O operation
    /// @param handler The handler for consuming the result
    @Override
    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {
        handler.failed(new UnsupportedOperationException("Lock operations are not supported"), attachment);
    }

    // Note: We don't override lock() because it's final in AsynchronousFileChannel

    /// Tells whether or not this channel is open.
    ///
    /// @return true if, and only if, this channel is open
    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    /// Closes this channel.
    /// Also closes the associated MerklePainter.
    ///
    /// @throws IOException If an I/O error occurs
    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            if (painter != null) {
                painter.close();
            }
        }
    }

    /// Asynchronously prebuffers a range of bytes by ensuring all chunks containing the range are downloaded.
    /// This method does not block and returns immediately with a CompletableFuture that completes when
    /// all chunks in the range are available.
    ///
    /// @param position The starting position in the file
    /// @param length The number of bytes to prebuffer
    /// @return A CompletableFuture that completes when all chunks in the range are available
    public CompletableFuture<Void> prebuffer(long position, long length) {
        if (position < 0) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IOException("Negative position: " + position));
            return future;
        }

        if (length < 0) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IOException("Negative length: " + length));
            return future;
        }

        try {
            if (position >= painter.totalSize()) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new IOException("Position beyond file size: " + position + " >= " + painter.totalSize()));
                return future;
            }

            // Adjust length if it would go beyond the end of the file
            long endPosition = Math.min(position + length, painter.totalSize());

            // Delegate to the painter's paintAsync method
            DownloadProgress progress = painter.paintAsync(position, endPosition);

            // Convert the DownloadProgress to a CompletableFuture<Void>
            CompletableFuture<Void> future = new CompletableFuture<>();

            // Set up a callback to complete the future when the download is done
            CompletableFuture.runAsync(() -> {
                try {
                    DownloadResult result = progress.get();
                    if (!result.isSuccess()) {
                        future.completeExceptionally(new IOException("Download failed: " + 
                            (result.error() != null ? result.error().getMessage() : "Unknown error")));
                    } else {
                        future.complete(null);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    future.completeExceptionally(e);
                }
            });

            return future;
        } catch (Exception e) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    /// Truncates this channel's file to the given size.
    /// This operation is not supported as the file is managed by the MerklePainter.
    ///
    /// @param size The new size, a non-negative byte count
    /// @return This channel
    /// @throws UnsupportedOperationException Always thrown as this operation is not supported
    @Override
    public AsynchronousFileChannel truncate(long size) {
        throw new UnsupportedOperationException("Truncate operations are not supported");
    }

    /// Forces any updates to this channel's file to be written to the storage device.
    /// This operation is delegated to the underlying channel.
    ///
    /// @param metaData If true then this method ensures that updates to both the file's content and metadata are written to storage; otherwise, only updates to the file's content are guaranteed to be written
    /// @throws IOException If an I/O error occurs
    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    /// Attempts to acquire a lock on a region of this channel's file.
    /// This operation is not supported as the file is managed by the MerklePainter.
    ///
    /// @param position The position at which the locked region is to start
    /// @param size The size of the locked region
    /// @param shared true if the lock is to be shared, false if it is to be exclusive
    /// @return A FileLock object, or null if the lock could not be acquired because another program holds an overlapping lock
    /// @throws UnsupportedOperationException Always thrown as this operation is not supported
    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException("Lock operations are not supported");
    }
}
