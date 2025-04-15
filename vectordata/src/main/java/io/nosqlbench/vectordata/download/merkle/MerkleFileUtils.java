package io.nosqlbench.vectordata.download.merkle;

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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Utility class for creating and managing Merkle tree files.
/// Creates Merkle tree files adjacent to source files with a ".merkle" extension.
public class MerkleFileUtils {
  private static final String MERKLE_EXTENSION = ".merkle";
  static final long DEFAULT_MIN_SECTION = 1024 * 1024 * 16;      // 16MiB
  static final long DEFAULT_MAX_SECTION = DEFAULT_MIN_SECTION * 4;     // 64KB

  /// Creates a Merkle tree file for the given source file using default section sizes.
  ///
  /// @param sourcePath the path to the source file
  /// @return the path to the created Merkle tree file
  /// @throws IOException if there are file operations errors
  public static Path createMerkleFile(Path sourcePath) throws IOException {
    return createMerkleFile(sourcePath, DEFAULT_MIN_SECTION, DEFAULT_MAX_SECTION);
  }

  /// Creates a Merkle tree file for the given source file with specified section sizes.
  ///
  /// @param sourcePath the path to the source file
  /// @param minSection minimum section size in bytes
  /// @param maxSection maximum section size in bytes
  /// @return the path to the created Merkle tree file
  /// @throws IOException if there are file operations errors
  public static Path createMerkleFile(Path sourcePath, long minSection, long maxSection)
      throws IOException
  {
    Path merklePath = getMerkleFilePath(sourcePath);

    // Create the Merkle tree
    ByteBuffer merkleTree = MerkleTreeBuilder.buildMerkleTree(sourcePath, minSection, maxSection);

    // Write the Merkle tree to file
    try (FileChannel channel = FileChannel.open(
        merklePath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING
    ))
    {
      while (merkleTree.hasRemaining()) {
        channel.write(merkleTree);
      }
    }

    return merklePath;
  }

  /// Gets the path where the Merkle tree file should be stored for a given source file.
  ///
  /// @param sourcePath the path to the source file
  /// @return the path where the Merkle tree file should be stored
  public static Path getMerkleFilePath(Path sourcePath) {
    return Path.of(sourcePath.toString() + MERKLE_EXTENSION);
  }

  /// Checks if a Merkle tree file exists for the given source file.
  ///
  /// @param sourcePath the path to the source file
  /// @return true if a Merkle tree file exists, false otherwise
  public static boolean hasMerkleFile(Path sourcePath) {
    return getMerkleFilePath(sourcePath).toFile().exists();
  }

  /// Deletes the Merkle tree file for the given source file if it exists.
  ///
  /// @param sourcePath the path to the source file
  /// @return true if the file was deleted, false if it didn't exist
  public static boolean deleteMerkleFile(Path sourcePath) {
    return getMerkleFilePath(sourcePath).toFile().delete();
  }
}
