package io.nosqlbench.nbvectors.buildhdf5;

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
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/// A supplier for JSON lines from a file
public class JJQSupplier {

  /// create a supplier for JSON lines from a file
  /// @param trainingJsonFile the path to the file to read from
  /// @return a supplier for JSON lines from a file
  public static Supplier<String> path(Path trainingJsonFile) {
    try {
      List<String> lines = Files.readAllLines(trainingJsonFile);
      return new LinesSupplier(lines);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// A supplier for JSON lines from a file
  public static class LinesSupplier implements Supplier<String> {

    private final Iterator<String> iterator;

    /// create a supplier for JSON lines from a file
    /// @param source the source of JSON lines
    public LinesSupplier(Iterable<String> source) {
      this.iterator = source.iterator();
    }

    @Override
    public String get() {
      if (iterator.hasNext()) {
        return iterator.next();
      }
      return null;
    }
  }
}
