package io.nosqlbench.nbvectors.jjq.apis;

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


import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.*;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.path.Path;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/// This impl serves as a bridge between the shared state between concurrent JJQ processing threads
/// and Scope lifecycle a la jackson-jq.
///
/// Ideally, a more directly supported and open mechanism should be used, but that would
/// require a change to jackson-jq to make it support easier extension.
///
/// Since there are no programmatic places to put any app-side state easily into the [Scope], this
/// function is created which serves this purpose. It implements [NBStateContext] which provides
/// the essentials for concurrent processing and consistent post-analysis behavior during shutdown.
///
/// Most of the logic here is not expected to be used in hot code paths.
/// It is init logic, thus should be guarded by local state elsewhere to avoid wasted cycles.

//@AutoService(Function.class)
@BuiltinFunction("nbstate/0")
public class NBStateContextHolderHack implements NBStateContext, Function, AutoCloseable {
  ///  When this context is shutdown, it will shutdown other objects which have also been
  /// registered for consistent shutdown. It will do this in the reverse order of registration.
  private final Deque<StatefulShutdown> statefulShutdowns = new ArrayDeque<>();

  /// A generic state map. Users of this map are expected to initialize needed state consistently
  /// and lazily, with appropirate type casts as needed on put and get.
  private final ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();

  /// List of all registered functions. This is done once per function, upon first invocation of
  /// a function implementation. If the function is not already registered, then the
  /// [NBBaseJQFunction#start] method is also called (just once).
  private final List<NBBaseJQFunction> registered = new ArrayList();

  /// create a new state context
  public NBStateContextHolderHack() {
    System.err.println("initializing new state cache for NB Functions");
  }

  /// {@inheritDoc}
  @Override
  public void apply(
      Scope scope,
      List<Expression> args,
      JsonNode in,
      Path path,
      PathOutput output,
      Version version
  ) throws JsonQueryException
  {
    throw new RuntimeException("This function should never be called. If you see this, then there"
                               + " is a programming error, or someone has used the 'nbstate' "
                               + "function directly, which is not allowed.");
  }

  /// {@inheritDoc}
  @Override
  public synchronized void register(NBBaseJQFunction f) {
    this.registered.add(f);
  }

  /// {@inheritDoc}
  @Override
  public synchronized List<NBBaseJQFunction> getRegisteredFunctions() {
    return registered;
  }

  /// {@inheritDoc}
  @Override
  public synchronized ConcurrentHashMap<String, Object> getState() {
    return state;
  }

  /// {@inheritDoc}
  @Override
  public synchronized void registerShutdownHook(StatefulShutdown shutdownable) {
    this.statefulShutdowns.add(shutdownable);
  }

  /// {@inheritDoc}
  @Override
  public synchronized void close() throws Exception {
    for (StatefulShutdown statefulShutdown : statefulShutdowns.reversed()) {
      System.out.println("shutting down " + statefulShutdown);
      statefulShutdown.shutdown();
    }
  }
}
