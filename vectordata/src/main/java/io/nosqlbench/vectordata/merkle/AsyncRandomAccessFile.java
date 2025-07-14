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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * An implementation of RandomAccessIO based on AsynchronousFileChannel.
 * This class provides random access to a file using Java NIO's asynchronous file operations.
 */
public class AsyncRandomAccessFile implements RandomAccessIO {

    private final AsynchronousFileChannel channel;
    private long position = 0;
    private final Path filePath;

    /**
     * Creates a new AsyncRandomAccessFile with a local path.
     *
     * @param localPath Path where the local data file exists
     * @throws IOException If there's an error opening the file
     */
    public AsyncRandomAccessFile(Path localPath) throws IOException {
        this.filePath = localPath;
        this.channel = AsynchronousFileChannel.open(localPath, StandardOpenOption.READ);
    }

    /**
     * Prebuffer a byte range in the file asynchronously.
     * For local files, this is a no-op that returns a completed future.
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
     * Returns the length of this file.
     *
     * @return The length of the file
     */
    @Override
    public long length() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeException("Error getting file length", e);
        }
    }

    /**
     * Seek to a position in the file.
     *
     * @param pos The position to seek to
     * @throws IOException if the seek operation fails
     */
    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IOException("Negative seek position: " + pos);
        }

        if (pos > length()) {
            throw new IOException("Seek position beyond file size: " + pos + " > " + length());
        }

        this.position = pos;
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
        return read(b, 0, b.length);
    }

    /**
     * Read up to len bytes of data from this file into the specified byte array.
     *
     * @param b The buffer into which the data is read
     * @param off The start offset in the buffer at which the data is written
     * @param len The maximum number of bytes to read
     * @return The total number of bytes read into the buffer, or -1 if the end of the file is reached
     * @throws IOException If an I/O error occurs
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= length()) {
            return -1;
        }

        ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
        try {
            Future<Integer> future = channel.read(buffer, position);
            int bytesRead = future.get();
            if (bytesRead > 0) {
                position += bytesRead;
            }
            return bytesRead;
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("Failed to read from file", e);
        }
    }

    /**
     * Reads a byte of data from this file.
     *
     * @return The next byte of data, or -1 if the end of the file is reached
     * @throws IOException If an I/O error occurs
     */
    public int read() throws IOException {
        byte[] b = new byte[1];
        int bytesRead = read(b);
        if (bytesRead == -1) {
            return -1;
        }
        return b[0] & 0xff;
    }

    /**
     * Closes this file and releases any system resources associated with it.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    // DataInput interface methods

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
            int count = read(b, off + bytesRead, len - bytesRead);
            if (count < 0) {
                throw new IOException("End of file reached before reading fully");
            }
            bytesRead += count;
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        long oldPosition = position;
        long newPosition = Math.min(position + n, length());
        position = newPosition;
        return (int)(newPosition - oldPosition);
    }

    @Override
    public boolean readBoolean() throws IOException {
        int ch = read();
        if (ch < 0) {
            throw new IOException("End of file reached");
        }
        return (ch != 0);
    }

    @Override
    public byte readByte() throws IOException {
        int ch = read();
        if (ch < 0) {
            throw new IOException("End of file reached");
        }
        return (byte)(ch);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int ch = read();
        if (ch < 0) {
            throw new IOException("End of file reached");
        }
        return ch;
    }

    @Override
    public short readShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0) {
            throw new IOException("End of file reached");
        }
        return (short)((ch1 << 8) + ch2);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0) {
            throw new IOException("End of file reached");
        }
        return (ch1 << 8) + ch2;
    }

    @Override
    public char readChar() throws IOException {
        int ch1 = read();
        int ch2 = read();
        if ((ch1 | ch2) < 0) {
            throw new IOException("End of file reached");
        }
        return (char)((ch1 << 8) + ch2);
    }

    @Override
    public int readInt() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new IOException("End of file reached");
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    @Override
    public long readLong() throws IOException {
        return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        StringBuilder input = new StringBuilder();
        int c;

        while (((c = read()) != -1) && (c != '\n')) {
            if (c != '\r') {
                input.append((char)c);
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }

        return input.toString();
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("readUTF not implemented");
    }

    // DataOutput interface methods - all throw UnsupportedOperationException as this is a read-only implementation

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void write(byte[] b) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeByte(int v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeShort(int v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeChar(int v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeInt(int v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeLong(long v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeFloat(float v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeDouble(double v) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("Write operations are not supported");
    }
}