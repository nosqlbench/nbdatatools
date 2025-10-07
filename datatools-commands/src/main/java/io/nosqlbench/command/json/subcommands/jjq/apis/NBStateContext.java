package io.nosqlbench.command.json.subcommands.jjq.apis;

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


import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/// A state context interface for NB functions
public interface NBStateContext extends AutoCloseable {

  /// register a function with this context
  /// @param f the function to register
  void register(NBBaseJQFunction f);

  /// get the list of registered functions
  /// @return the list of registered functions
  List<NBBaseJQFunction> getRegisteredFunctions();

  /// get the state map for this context
  /// @return the state map for this context
  ConcurrentHashMap<String, Object> getState();

  /// register a shutdown hook with this context
  /// @param statefulShutdown the shutdown hook to register
  void registerShutdownHook(StatefulShutdown statefulShutdown);
}
