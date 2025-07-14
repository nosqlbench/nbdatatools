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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 Setup a merkle pane, an auxiliary view of merkle state attached to a file and available ranges of
 data. This class centralizes the setup logic for MerklePane instances. */
public class MerklePaneSetup {

  /// Construct a new MerklePaneSetup.
  public MerklePaneSetup() {
  }

  private final static Logger logger = LogManager.getLogger(MerklePaneSetup.class);

  /// This should initialize the merkle tree and content file state, given four parameters.
  /// When copied from a remote location, the remote merkle file contents become a local
  /// reference merkle file.
  /// # REQUIREMENTS:
  /// 1. The only thing required to have a valid starting scenario is an accessible
  /// remoteContentPath and an associated remoteMerklePath.
  /// 2. The first step of initialization is to ensure that a local MREF merkle reference file is
  ///  the same as the remote merkle file. If there is no local merkle file, then the remote MRKL
  /// merkle file is fetched and stored in the local MREF file location. If there is a local MREF
  ///  file, then the remote merkle file footer is fetched with a ranged read, and compared to
  /// the footer read from the local MREF file. If the footer is the same, then the local
  /// file can be left as is and there is no required sync from remote MRKL to local MREF.
  /// 3. If the local merkle file and content are present, and the local content file is older
  /// than the local merkle file, then both are left as is. If the local content file has a newer
  ///  mtime than the local merkle file, then the local merkle file is considered stale and must
  /// be recomputed from the local content file.
  /// 4. If the local content file does not exist, then a zero-length file is made as a place
  /// holder and the local merkle file is initialized as "new" with all hashes being set to dirty.
  ///
  /// The goal of this is to avoid downloading the whole content file if necessary, thus the
  /// local MRKL file is used to compare contents of the local content file to the remote view of
  ///  the merkle tree which is stored in the MREF file.
  ///
  /// ## Errors
  ///
  /// If the download fails, then an exception should be thrown.
  /// If the merkle reference file (MREF) does not pass its self-check digest verifacation on load,
  /// then it should be discarded and a new download should be attempted. There should be up to 3
  ///  tries before an exception is finally thrown in this case. In every other case that is not
  /// documented in this javadoc, an exception should be thrown.
  /// @param localContentPath
  ///     The path of the local content file to be fetched incrementall by
  ///                                     other classes acting on this class.
  /// @param localMerklePath
  ///     the local merkle tree file path which represents the state of the local
  ///                                     content path.
  /// @param remoteContentPath
  ///     the remote path of the content which will be fetched with range requests to fill in the
  ///         local content path, as a result of other classes acting on this class
  /// @return a MerkleTree instance
  public static MerkleTree initTree(
      Path localContentPath,
      Path localMerklePath,
      String remoteContentPath
  )
  {
    // Validate arguments: all parameters must be provided
    if (localContentPath == null || localMerklePath == null) {
      throw new IllegalArgumentException("Content path and Merkle path must be non-null");
    }

    // According to requirements, it should be invalid to create a MerklePane without a remote content URL
    if (remoteContentPath == null) {
      throw new IllegalArgumentException("Remote content URL must be provided");
    }

    // Remote content URL is provided
    // Determine if remote content path should be treated as remote (HTTP) or local (file)
    boolean hasRemoteContent;
    try {
      java.net.URI uri = new java.net.URI(remoteContentPath);
      String scheme = uri.getScheme();
      hasRemoteContent = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
    } catch (Exception e) {
      hasRemoteContent = false;
    }
    try {
      // Ensure parent directories
      Files.createDirectories(localContentPath.getParent());
      Files.createDirectories(localMerklePath.getParent());
      // Step 1: Handle remote reference Merkle tree if remoteContentPath provided
      Path localMrefPath =
          localContentPath.resolveSibling(localContentPath.getFileName().toString() + ".mref");

      if (hasRemoteContent) {
        // Check if remote content file is accessible
        URL remoteContentUrl = new URI(remoteContentPath).toURL();
        logger.debug("remote content path: {}", remoteContentUrl.toString());


        long contentSize = getSizeOfRemoteResource(remoteContentUrl);
        logger.debug("remote content size: {}", contentSize);
        if (contentSize == 0) {
          throw new RuntimeException("invalid remote content size: " + contentSize + " bytes for "
                                     + remoteContentUrl.toString());
        }

        // Check if remote merkle tree file is accessible
        URL remoteMrefUrl = new URI(remoteContentPath + ".mrkl").toURL();
        logger.debug("remote merkle tree path: {}", remoteMrefUrl.toString());
        long merkleSize = getSizeOfRemoteResource(remoteMrefUrl);
        logger.debug("remote merkle tree size: {}", merkleSize);
        if (merkleSize == 0) {
          throw new RuntimeException("invalid remote merkle tree size: " + merkleSize + " bytes for "
                                     + remoteMrefUrl.toString());
        }

        IOException lastEx = null;
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            if (Files.exists(localMrefPath) && Files.size(localMrefPath) > 0) {
              MerkleTree.load(localMrefPath);
              break;
            }
            Files.createDirectories(localMrefPath.getParent());
            try (InputStream in = remoteMrefUrl.openStream();
                 OutputStream out = Files.newOutputStream(localMrefPath))
            {
              in.transferTo(out);
            }
            MerkleTree.load(localMrefPath);
            break;
          } catch (IOException e) {
            logger.warn(
                "Failed to obtain a valid merkle tree reference (attempt {}/{}): {}",
                attempt,
                maxAttempts,
                e.getMessage(),
                e
            );
            Files.deleteIfExists(localMrefPath);
            lastEx = e;
          }
        }
        if (!Files.exists(localMrefPath)) {
          throw new IOException(
              "Failed to obtain valid reference Merkle tree after 3 attempts: " + (
                  lastEx == null ? "unknown error" : lastEx.getMessage()), lastEx
          );
        }
      } else {
        // For local-only setup, create a reference merkle tree from the local content file
        if (!Files.exists(localMrefPath) || Files.size(localMrefPath) == 0) {
          // Ensure content file exists
          if (!Files.exists(localContentPath)) {
            Files.createFile(localContentPath);
          }

          // Create a minimal merkle tree for the content file
          MerkleTreeBuildProgress progress = MerkleTree.fromData(localContentPath);
          MerkleTree refTree = progress.getFuture().join();
          refTree.save(localMrefPath);
        }
      }
      // Ensure local MRKL exists (create empty if missing)
      if (!Files.exists(localMerklePath) || Files.size(localMerklePath) == 0) {
        MerkleTree.createEmptyTreeLike(localMrefPath, localMerklePath);
      }
      // Ensure content file exists
      if (!Files.exists(localContentPath)) {
        Files.createFile(localContentPath);
      }

      // Check if content file is newer than merkle file, rebuild if necessary
      if (Files.exists(localContentPath) && Files.exists(localMerklePath)) {
        try {
          long contentLastModified = Files.getLastModifiedTime(localContentPath).toMillis();
          long merkleLastModified = Files.getLastModifiedTime(localMerklePath).toMillis();

          if (contentLastModified > merkleLastModified) {
            logger.debug("Content file is newer than merkle file, rebuilding merkle tree");
            MerkleTreeBuildProgress progress = MerkleTree.fromData(localContentPath);
            MerkleTree newTree = progress.getFuture().join();
            newTree.save(localMerklePath);
          }
        } catch (IOException e) {
          logger.warn("Failed to check file modification times: {}", e.getMessage(), e);
        }
      }

      // Load and return primary Merkle tree
      return MerkleTree.load(localMerklePath);
    } catch (IOException | URISyntaxException e) {
      logger.error(e.getMessage(),e);
      throw new RuntimeException(e);
    }
  }

  /// Creates and sets up a complete MerklePane with the given parameters.
  /// This method centralizes the setup logic for MerklePane instances.
  /// @param localContentPath The path of the local content file
  /// @param localMerklePath The path of the local merkle tree file
  /// @param remoteContentPath The remote path of the content (must be provided)
  /// @return A fully initialized MerklePane instance
  /// @throws RuntimeException If setup fails for any reason
  public static MerklePane setupMerklePane(
      Path localContentPath,
      Path localMerklePath,
      String remoteContentPath
  ) {
    // Initialize the merkle tree
    MerkleTree merkleTree = initTree(localContentPath, localMerklePath, remoteContentPath);

    // Create the reference tree path
    Path referenceTreePath = localContentPath.resolveSibling(localContentPath.getFileName().toString() + ".mref");

    // Create and return the MerklePane
    return new MerklePane(localContentPath, localMerklePath, referenceTreePath, remoteContentPath);
  }

  ///  Get the size of the remote resource using a GET request.
  /// This method uses a GET request instead of a HEAD request to avoid potential issues with HEAD requests.
  /// @param remoteMrefUrl the remote url
  /// @return the size of the resource in bytes
  private static long getSizeOfRemoteResource(URL remoteMrefUrl) throws IOException {
    if ("file".equals(remoteMrefUrl.getProtocol())) {
      // For file URLs, get the size directly from the file
      try {
        Path filePath = Path.of(remoteMrefUrl.toURI());
        if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
          return Files.size(filePath);
        } else {
          throw new IOException("File not found or not a regular file: " + filePath);
        }
      } catch (java.net.URISyntaxException e) {
        throw new IOException("Invalid URI: " + remoteMrefUrl, e);
      }
    } else {
      // For HTTP URLs, use GET instead of HEAD to avoid potential issues with HEAD requests
      HttpURLConnection conn = (HttpURLConnection) remoteMrefUrl.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);
      conn.connect();

      int responseCode = conn.getResponseCode();
      if (responseCode < 200 || responseCode >= 300) {
        throw new IOException("Failed to get size of remote resource: " + remoteMrefUrl + 
                             ", HTTP response code: " + responseCode);
      }

      // Get the content length from the response headers
      long contentLength = conn.getContentLengthLong();

      // If content length is not available from headers, read the content and get its length
      if (contentLength < 0) {
        try (InputStream in = conn.getInputStream()) {
          byte[] content = in.readAllBytes();
          contentLength = content.length;
        }
      }

      return contentLength;
    }
  }


}
