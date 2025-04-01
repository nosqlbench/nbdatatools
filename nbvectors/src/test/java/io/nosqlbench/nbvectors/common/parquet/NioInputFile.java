package io.nosqlbench.nbvectors.common.parquet;

//import org.apache.parquet.io.InputFile;
//import org.apache.parquet.io.SeekableInputStream;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.SeekableByteChannel;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.StandardOpenOption;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class NioInputFile
{
//    implements InputFile
//
//    private final Path path;
//
//    public NioInputFile(Path path) {
//        this.path = path;
//    }
//
//    @Override
//    public long getLength() throws IOException {
//        return Files.size(path);
//    }
//
//    @Override
//    public SeekableInputStream newStream() throws IOException {
//        SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ);
//        return new SeekableByteChannelInputStream(channel);
//    }
//
//    public static class SeekableByteChannelInputStream extends SeekableInputStream {
//        private final SeekableByteChannel channel;
//
//        public SeekableByteChannelInputStream(SeekableByteChannel channel) {
//            this.channel = channel;
//        }
//
//        @Override
//        public long getPos() throws IOException {
//            return channel.position();
//        }
//
//        @Override
//        public void seek(long newPos) throws IOException {
//            channel.position(newPos);
//        }
//
//        @Override
//        public int read() throws IOException {
//            byte[] b = new byte[1];
//            int read = read(b);
//            if (read == 1) {
//                return b[0] & 0xFF;
//            } else {
//                return -1;
//            }
//        }
//
//        @Override
//        public int read(byte[] b) throws IOException {
//            return read(b, 0, b.length);
//        }
//
//        @Override
//        public int read(byte[] b, int off, int len) throws IOException {
//            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
//            return channel.read(buffer);
//        }
//
//        @Override
//        public int read(ByteBuffer buf) throws IOException {
//            return channel.read(buf);
//        }
//
//        @Override
//        public void readFully(byte[] b) throws IOException {
//            readFully(b, 0, b.length);
//        }
//
//        @Override
//        public void readFully(byte[] b, int off, int len) throws IOException {
//            ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
//            int read = 0;
//            while (read < len) {
//                int r = channel.read(buffer);
//                if (r < 0) {
//                    throw new IOException("End of stream reached before reading all bytes.");
//                }
//                read += r;
//            }
//        }
//
//        @Override
//        public void readFully(ByteBuffer buf) throws IOException {
//            int read = 0;
//            while (read < buf.remaining()) {
//                int r = channel.read(buf);
//                if (r < 0) {
//                    throw new IOException("End of stream reached before reading all bytes.");
//                }
//                read += r;
//            }
//        }
//
//        @Override
//        public long skip(long n) throws IOException {
//            long currentPosition = channel.position();
//            long newPosition = Math.min(currentPosition + n, channel.size());
//            channel.position(newPosition);
//            return newPosition - currentPosition;
//        }
//
//        @Override
//        public void close() throws IOException {
//            channel.close();
//        }
//    }
}