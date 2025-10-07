package io.nosqlbench.command.json.subcommands.jjq.contexts;

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


import io.nosqlbench.command.json.subcommands.jjq.apis.StatefulShutdown;

/// A stateful context interface for mapping string values to long values for specific field names
public interface NBIdMapper extends StatefulShutdown {

  /// add an instance of a string value to the context
  /// @param fieldName the name of the field to add the instance to
  /// @param string the string value to add
  void addInstance(String fieldName, String string);

  /// lookup the id for a string value
  /// @param fieldName the name of the field to lookup the id for
  /// @param text the string value to lookup
  /// @return the id for the string value
  long lookupId(String fieldName, String text);
}
