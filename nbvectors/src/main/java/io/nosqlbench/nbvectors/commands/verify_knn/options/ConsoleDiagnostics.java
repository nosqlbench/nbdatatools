package io.nosqlbench.nbvectors.commands.verify_knn.options;

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


import org.apache.logging.log4j.Level;

/// Adapt standard diagnostic levels from a library-neutral type into a library-specific type,
/// in this case log4j {@link Level}
public enum ConsoleDiagnostics {
  /// fatal, any error thrown at this level is presumed to be bad enough to force a shutdown
  FATAL(Level.FATAL),
  /// normal errors that users should see, after which normal operation is not possible
  ERROR(Level.ERROR),
  /// warnings, which are advisory but not a reason to stop running
  WARN(Level.WARN),
  /// routine events that inform a user about what is happening
  INFO(Level.INFO),
  /// play-by-play details which most users don't want unless they are trying to understand what
  /// went wrong
  DEBUG(Level.DEBUG),
  /// the finest grain of diagnostic data, use this sparingly, as it is almost never necessary
  TRACE(Level.TRACE);

  private final Level level;

  /// create a console diagnostic level
  ConsoleDiagnostics(Level fatal) {
    this.level = fatal;
  }

  /// get the log4j level for this diagnostic level
  /// @return the log4j level
  public Level getLevel() {
    return level;
  }
}
