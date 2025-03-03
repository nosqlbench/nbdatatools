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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Thread.*;

public class ConcurrentSupplier<T> implements Supplier<T>, AutoCloseable {
  private final Iterator<T> source;
  private final LinkedBlockingDeque<T> buffer;
  private final Consumer<RuntimeException> errorChannel;
  private volatile boolean priming = true;
  private Thread feeder;

  public ConcurrentSupplier(
      Iterable<T> source,
      int bufferSize,
      Consumer<RuntimeException> errorChannel
  )
  {
    this(source.iterator(), bufferSize);
  }

  public ConcurrentSupplier(Iterator<T> iterator, int bufferSize) {
    this.source = iterator;
    this.errorChannel = null;
    this.buffer = new LinkedBlockingDeque<>(bufferSize);
    startFeeder();
  }

  private void startFeeder() {
    this.feeder = ofVirtual().name("feeder").factory().newThread(() -> {
      while (source.hasNext()) {
        T next = source.next();
        try {
          buffer.putFirst(next);
        } catch (InterruptedException e) {
        }
      }
      priming=false;
    });
    feeder.start();
  }

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

  /// Blocking read on the queue which will keep trying unless there is not1hing to read and
  /// nothing being supplied.
  public T get() {
    T value = null;
    while (priming||!buffer.isEmpty()) {
      try {
        value = this.buffer.pollFirst(100,TimeUnit.MILLISECONDS);
        if (value!=null) {
          break;
        }
      } catch (InterruptedException e) {
      }
    }
    return value;
  }
}
