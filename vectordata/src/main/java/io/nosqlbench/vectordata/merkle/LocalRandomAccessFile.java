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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * A thin wrapper around RandomAccessFile that implements BufferedRandomAccessFile
 * with no-op implementations for the prebuffering methods.
 * This is used for local filesystem resources where prebuffering is not needed.
 */
public class LocalRandomAccessFile extends RandomAccessFile implements BufferedRandomAccessFile {

    /**
     * Creates a new LocalRandomAccessFile with a local path.
     *
     * @param localPath Path where the local data file exists
     * @throws IOException If there's an error opening the file
     */
    public LocalRandomAccessFile(Path localPath) throws IOException {
        super(localPath.toFile(), "r");
    }

    /**
     * No-op implementation of prebuffer for local files.
     * Returns a completed future since no prebuffering is needed for local files.
     *
     * @param position The starting position in the file
     * @param length The number of bytes to prebuffer
     * @return A completed CompletableFuture
     */
    @Override
    public CompletableFuture<Void> prebuffer(long position, long length) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Reads bytes into the provided array.
     *
     * @param b The buffer into which the data is read
     * @return The total number of bytes read into the buffer, or -1 if the end of the file is reached
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int read(byte[] b) throws IOException {
        return super.read(b);
    }

    /**
     * Returns the length of this file.
     * Overrides the method in RandomAccessFile to match the BufferedRandomAccessFile interface.
     *
     * @return The length of the file
     */
    @Override
    public long length() {
        try {
            return super.length();
        } catch (IOException e) {
            throw new RuntimeException("Error getting file length", e);
        }
    }
}
