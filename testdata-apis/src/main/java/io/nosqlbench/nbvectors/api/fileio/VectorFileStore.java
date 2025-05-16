package io.nosqlbench.nbvectors.api.fileio;

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


import io.nosqlbench.nbvectors.api.noncore.VectorStreamStore;

import java.nio.file.Path;

public interface VectorFileStore<T> extends VectorStreamStore<T>  {
  /**
   Initialize the writer with a path
   @param path
   The path to initialize the writer with
   */
  public void open(Path path);

  default void flush() {};

  /// This should be called at the end of the writer lifecycle.
  void close();
}
