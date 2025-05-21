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


import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/// Capture the interface of a RandomAccessFile
/// Ideally, all the methods on RandomAccessFile would belong to some faceted interface for easy
/// extension, but instead we reify the gaps in its signature here.
public interface BufferedRandomAccessFile extends DataOutput, DataInput, Closeable {
  CompletableFuture<Void> prebuffer(long position, long length);
  void awaitPrebuffer(long minIncl, long maxExcl);
  long length();
  void seek(long i) throws IOException;
  int read(byte[] dimBytes) throws IOException;
}
