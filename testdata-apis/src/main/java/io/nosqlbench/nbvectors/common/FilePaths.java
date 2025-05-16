package io.nosqlbench.nbvectors.common;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;

/// A collection of file path utilities for the nbvectors tool
public class FilePaths {

  /// relink a path to a new path
  /// @param fromPath the path to relink
  /// @param toPath the path to relink to
  /// @return the path to the relinked file
  public static Path relinkPath(Path fromPath, Path toPath) {
    try {
      Path parent = toPath.getParent();
      if (parent!=null) {
        Files.createDirectories(
            parent,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"))
        );
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      Files.move(
          fromPath,
          toPath,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return toPath;
  }

}
