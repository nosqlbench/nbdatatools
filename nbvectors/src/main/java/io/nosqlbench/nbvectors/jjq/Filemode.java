package io.nosqlbench.nbvectors.jjq;

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


import java.nio.file.Files;
import java.nio.file.Path;

public enum Filemode {
  /// If a function has an output file option, and the file exists already, then presume that the
  /// function ran successfully before
  checkpoint,
  /// Overwrite any output files, truncating them first if they exist
  overwrite,
  /// If an output file already exists, cancel the operation and throw an error
  preserve;

  public boolean isSkip(Path path) {
    if (Files.exists(path)) {
      return switch (this) {
        case preserve -> throw new RuntimeException(
            "file 'path' exists, and " + this.getClass().getSimpleName() + "=" + this);
        case overwrite -> false;
        case checkpoint -> true;
      };
    } else {
      return false;
    }
  }
}
