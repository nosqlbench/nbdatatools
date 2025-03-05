package io.nosqlbench.nbvectors.jjq.bulkio;

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


import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Thread.*;

/// Adapt a potentially non-threadsafe source of data into a threadsafe version
/// which actively reads and primes the buffer from the upstream source. This can be thought of
/// as a simple single-producer/multiple-consumer concurrent queue, just through the
///  {@link Supplier} interface.
/// @param <T> The buffered type
public class ConcurrentSupplier<T> implements Supplier<T>, AutoCloseable {
  private final Iterator<T> source;
  private final LinkedBlockingDeque<T> buffer;
  private final Consumer<RuntimeException> errorChannel;
  private volatile boolean priming = true;

  /// create a concurrent supplier
  /// @param bufferSize the size of the buffer; determines blocking limit on input buffering
  /// @param source the data source
  /// @param errorChannel A receiver of errors, sideband channel
  public ConcurrentSupplier(
      Iterable<T> source,
      int bufferSize,
      Consumer<RuntimeException> errorChannel
  )
  {
    this(source.iterator(), bufferSize, errorChannel);
  }

  /// like {@link ConcurrentSupplier#ConcurrentSupplier(Iterable, int, Consumer)}, but for an
  /// iterator of {@link T}
  /// @param bufferSize the size of the buffer; determines blocking limit on input buffering
  /// @param iterator the source of {@link T} object
  /// @param errorChannel A receiver of errors, sideband channel
  public ConcurrentSupplier(Iterator<T> iterator, int bufferSize, Consumer<RuntimeException> errorChannel) {
    this.source = iterator;
    this.errorChannel = errorChannel;
    this.buffer = new LinkedBlockingDeque<>(bufferSize);
    startFeeder();
  }

  private void startFeeder() {
    Thread feeder = ofVirtual().name("feeder").factory().newThread(() -> {
      while (source.hasNext()) {
        T next = source.next();
        try {
          buffer.putFirst(next);
        } catch (InterruptedException ignored) {
        }
      }
      priming = false;
    });
    feeder.start();
  }

  /// Close this concurrent supplier.
  /// @throws RuntimeException when this buffer is closes while not empty, and no error consumer
  /// has been provided
  @Override
  public void close() throws Exception {
    this.priming = false;
    if (!buffer.isEmpty()) {
      throwOrReport(new RuntimeException(
          "queue was closed with " + buffer.size() + " pending elements"));
    }
  }

  private void throwOrReport(RuntimeException error) {
    if (errorChannel == null) {
      throw error;
    } else {
      errorChannel.accept(error);
    }

  }

  /// Blocking read on the queue which will keep trying unless there is nothing to read and
  /// nothing being supplied.
  /// This tries by default ever 1/10 second, but this should probably
  /// be changed to a blocking read which can be broken with [Object#notify()]
  /// @return Either a {@link T} or null, if this source of data is empty and
  /// should be closed gracefully
  public T get() {
    T value = null;
    while (priming||!buffer.isEmpty()) {
      try {
        value = this.buffer.pollFirst(100,TimeUnit.MILLISECONDS);
        if (value!=null) {
          break;
        }
      } catch (InterruptedException ignored) {
      }
    }
    return value;
  }
}
