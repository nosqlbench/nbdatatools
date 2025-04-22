package io.nosqlbench.nbvectors.datasource.parquet.traversal.functional;

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


import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConvertingIterable;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/// A supplier for [InputFile] from a list of [Path]
public class InputFileSupplier implements Supplier<InputFile> {
  private final Iterator<InputFile> iterator;

  /// create a supplier for [InputFile] from a list of [Path]
  /// @param files the list of [Path] to read from
  public InputFileSupplier(List<Path> files) {

    ConvertingIterable<Path, InputFile> inputFileIterable =
        new ConvertingIterable<>(files, LocalInputFile::new);
    this.iterator = inputFileIterable.iterator();
  }

  @Override
  public InputFile get() {
    if (this.iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }

}
