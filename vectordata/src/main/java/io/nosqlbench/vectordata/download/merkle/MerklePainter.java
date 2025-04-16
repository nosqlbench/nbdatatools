package io.nosqlbench.vectordata.download.merkle;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;

/// Combines a MerklePane with source URL functionality for downloading and verifying data.
/// Downloads and manages the reference merkle tree from a source URL.
public class MerklePainter implements Closeable {
    private final MerklePane pane;
    private final String sourcePath;
    private final Path localPath;
    private final Path merklePath;
    private final Path referenceTreePath;

    /// Creates a new MerklePainter for the given local file and source URL
    ///
    /// @param localPath Path where the local data file exists or will be stored
    /// @param sourcePath URL of the source data file (merkle tree will be downloaded from sourcePath + ".mrkl")
    /// @throws IOException If there's an error opening the files or downloading reference data
    public MerklePainter(Path localPath, String sourcePath) throws IOException {
        this.localPath = localPath;
        this.sourcePath = sourcePath;
        this.merklePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mrkl");
        this.referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mref");

        // Download the reference merkle tree from source
        downloadReferenceMerkleTree();

        // Initialize the pane with local paths
        this.pane = new MerklePane(localPath, merklePath);
    }

    private void downloadReferenceMerkleTree() throws IOException {
        URL merkleUrl = new URL(sourcePath + ".mrkl");
        try (InputStream in = merkleUrl.openStream();
             FileOutputStream out = new FileOutputStream(referenceTreePath.toFile())) {
            in.transferTo(out);
        }
    }

    /// Gets the source URL of the data file
    ///
    /// @return The source URL
    public String sourcePath() {
        return sourcePath;
    }

    /// Gets the local path where the data file is stored
    ///
    /// @return The local file path
    public Path localPath() {
        return localPath;
    }

    /// Gets the path to the local merkle tree file
    ///
    /// @return The merkle tree file path
    public Path merklePath() {
        return merklePath;
    }

    /// Gets the path to the downloaded reference merkle tree file
    ///
    /// @return The reference merkle tree file path
    public Path referenceTreePath() {
        return referenceTreePath;
    }

    /// Gets the underlying MerklePane
    ///
    /// @return The MerklePane instance
    public MerklePane pane() {
        return pane;
    }

    @Override
    public void close() throws IOException {
        pane.close();
    }
}