package io.nosqlbench.vectordata.download.merkle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/// Utility class for creating, saving, and loading Merkle trees with metadata footers.
///
/// This class provides methods to create, load, and save Merkle tree files, which include
/// both the tree data and a metadata footer with information about the tree structure.
///
public class MerkleTreeFile {
    private static final int HASH_SIZE = 32; // SHA-256
    private final ByteBuffer treeData;
    private final MerkleTreeFooter footer;
    private final List<Long> leafBoundaries;

    private MerkleTreeFile(ByteBuffer treeData, MerkleTreeFooter footer, List<Long> leafBoundaries) {
        this.treeData = treeData;
        this.footer = footer;
        this.leafBoundaries = leafBoundaries;
    }

    /// Creates a new Merkle tree for the given file using default parameters.
    ///
    /// @param sourcePath the path to the source file
    /// @return a new MerkleTreeFile instance
    /// @throws IOException if there are file operations errors
    public static MerkleTreeFile create(Path sourcePath) throws IOException {
        return create(sourcePath, MerkleFileUtils.DEFAULT_MIN_SECTION, MerkleFileUtils.DEFAULT_MAX_SECTION);
    }

    /// Creates a new Merkle tree for the given file with custom section sizes.
    ///
    /// @param sourcePath the path to the source file
    /// @param minSection minimum section size in bytes
    /// @param maxSection maximum section size in bytes
    /// @return a new MerkleTreeFile instance
    /// @throws IOException if there are file operations errors
    public static MerkleTreeFile create(Path sourcePath, long minSection, long maxSection) throws IOException {
        try (FileChannel channel = FileChannel.open(sourcePath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            List<Long> boundaries = MerkleTreeRanger.computeMerkleOffsets(fileSize, minSection, maxSection);

            // Build the tree data
            ByteBuffer treeData = buildTreeData(channel, boundaries);

            // Create the footer
            MerkleTreeFooter footer = createFooter(fileSize, boundaries, treeData);

            return new MerkleTreeFile(treeData, footer, boundaries);
        }
    }

    /// Loads a Merkle tree from an existing file.
    ///
    /// @param merklePath the path to the Merkle tree file
    /// @return a loaded MerkleTreeFile instance
    /// @throws IOException if there are file operations errors
    public static MerkleTreeFile load(Path merklePath) throws IOException {
        try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
            // Read footer first
            long fileSize = channel.size();
            ByteBuffer footerBuffer = ByteBuffer.allocate(MerkleTreeFooter.size());
            channel.position(fileSize - MerkleTreeFooter.size());
            channel.read(footerBuffer);
            footerBuffer.flip();

            MerkleTreeFooter footer = MerkleTreeFooter.decode(footerBuffer);
            footer.validate();  // Validate immediately after decoding

            // Read leaf boundaries
            ByteBuffer boundariesBuffer = ByteBuffer.allocate(footer.leafBoundaryTableLength());
            channel.position(footer.leafBoundaryTableOffset());
            channel.read(boundariesBuffer);
            boundariesBuffer.flip();

            List<Long> boundaries = new ArrayList<>();
            while (boundariesBuffer.hasRemaining()) {
                boundaries.add(boundariesBuffer.getLong());
            }

            // Read tree data
            ByteBuffer treeData = ByteBuffer.allocate(footer.leafBoundaryTableOffset());
            channel.position(0);
            channel.read(treeData);
            treeData.flip();

            return new MerkleTreeFile(treeData, footer, boundaries);
        }
    }

    /// Saves this Merkle tree to a file.
    ///
    /// @param merklePath the path where the Merkle tree file will be saved
    /// @throws IOException if there are file operations errors
    public void save(Path merklePath) throws IOException {
        try (FileChannel channel = FileChannel.open(merklePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write tree data - ensure we're reading from the start
            ByteBuffer treeDataCopy = treeData.duplicate();
            treeDataCopy.position(0);
            channel.write(treeDataCopy);

            // Write leaf boundaries
            ByteBuffer boundariesBuffer = ByteBuffer.allocate(leafBoundaries.size() * 8);
            for (Long boundary : leafBoundaries) {
                boundariesBuffer.putLong(boundary);
            }
            boundariesBuffer.flip();
            channel.write(boundariesBuffer);

            // Write footer
            ByteBuffer footerBuffer = footer.encode();
            footerBuffer.flip();  // Ensure buffer is ready for reading
            channel.write(footerBuffer);
        }
    }

    private static ByteBuffer buildTreeData(FileChannel channel, List<Long> boundaries) throws IOException {
        List<byte[]> leafHashes = computeLeafHashes(channel, boundaries);

        // Calculate tree size
        int leafNodes = leafHashes.size();
        int totalNodes = 2 * leafNodes - 1;
        ByteBuffer treeBuffer = ByteBuffer.allocate(totalNodes * HASH_SIZE);

        // Build the tree bottom-up
        buildTree(treeBuffer, leafHashes);

        treeBuffer.flip();
        return treeBuffer;
    }

    /// Computes the hash values for each leaf section of the file.
    ///
    /// @param channel the file channel to read from
    /// @param boundaries the list of section boundary offsets
    /// @return a list of hash values for each leaf section
    /// @throws IOException if there are file operations errors
    private static List<byte[]> computeLeafHashes(FileChannel channel, List<Long> boundaries) throws IOException {
        List<byte[]> leafHashes = new ArrayList<>();
        MessageDigest digest = getMessageDigest();

        for (int i = 0; i < boundaries.size() - 1; i++) {
            long start = boundaries.get(i);
            long end = boundaries.get(i + 1);
            long length = end - start;

            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, 1024 * 1024));

            channel.position(start);
            digest.reset();

            while (channel.position() < end) {
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) break;

                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            leafHashes.add(digest.digest());
        }

        return leafHashes;
    }

    /// Builds the Merkle tree from the leaf hashes and writes it to the buffer.
    ///
    /// @param treeBuffer the buffer to write the tree to
    /// @param leafHashes the list of leaf node hashes
    private static void buildTree(ByteBuffer treeBuffer, List<byte[]> leafHashes) {
        List<byte[]> currentLevel = new ArrayList<>(leafHashes);
        MessageDigest digest = getMessageDigest();

        // Write leaf hashes
        for (byte[] hash : leafHashes) {
            treeBuffer.put(hash);
        }

        // Build upper levels
        while (currentLevel.size() > 1) {
            List<byte[]> nextLevel = new ArrayList<>();

            for (int i = 0; i < currentLevel.size() - 1; i += 2) {
                digest.reset();
                digest.update(currentLevel.get(i));
                digest.update(currentLevel.get(i + 1));
                byte[] parentHash = digest.digest();
                nextLevel.add(parentHash);
                treeBuffer.put(parentHash);
            }

            // Handle odd number of nodes
            if (currentLevel.size() % 2 == 1) {
                byte[] lastHash = currentLevel.get(currentLevel.size() - 1);
                nextLevel.add(lastHash);
                treeBuffer.put(lastHash);
            }

            currentLevel = nextLevel;
        }
    }

    /// Creates a footer for the Merkle tree file.
    ///
    /// @param fileSize the size of the original file
    /// @param boundaries the list of section boundary offsets
    /// @param treeData the Merkle tree data
    /// @return a new MerkleTreeFooter instance
    private static MerkleTreeFooter createFooter(long fileSize, List<Long> boundaries, ByteBuffer treeData) {
        int treeSize = treeData.limit();
        int boundariesSize = boundaries.size() * 8;

        return new MerkleTreeFooter(
            MerkleTreeFooter.CURRENT_VERSION,
            (byte) 0, // No flags set
            MerkleTreeFooter.HashAlgorithm.SHA_256.getValue(),
            MerkleTreeFooter.HashAlgorithm.SHA_256.getDigestLength(),
            fileSize,
            boundaries.size() - 1, // number of leaves
            treeSize, // leaf boundary table offset
            boundariesSize, // leaf boundary table length
            computeFileDigest(treeData), (short) MerkleTreeFooter.size()
        );
    }

    /// Computes a digest of the entire Merkle tree data.
    ///
    /// @param treeData the Merkle tree data
    /// @return the digest as a byte array
    private static byte[] computeFileDigest(ByteBuffer treeData) {
        MessageDigest digest = getMessageDigest();
        digest.update(treeData.duplicate());
        return digest.digest();
    }

    /// Gets a MessageDigest instance for the hash algorithm used by the Merkle tree.
    ///
    /// @return a MessageDigest instance
    /// @throws RuntimeException if the hash algorithm is not available
    private static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /// Gets the Merkle tree data as a read-only buffer.
    ///
    /// @return a read-only buffer containing the Merkle tree data
    public ByteBuffer getTreeData() {
        return treeData.asReadOnlyBuffer();
    }

    /// Gets the footer of the Merkle tree file.
    ///
    /// @return the MerkleTreeFooter instance
    public MerkleTreeFooter getFooter() {
        return footer;
    }

    /// Gets the list of leaf section boundaries.
    ///
    /// @return a copy of the list of leaf section boundaries
    public List<Long> getLeafBoundaries() {
        return new ArrayList<>(leafBoundaries);
    }
}