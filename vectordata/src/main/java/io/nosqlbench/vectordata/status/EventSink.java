package io.nosqlbench.vectordata.status;

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


/// Interface for handling download events and logging.
///
/// This interface provides methods for logging events at different levels
/// during the download process. Implementations can direct these events
/// to appropriate logging systems or output destinations.
public interface EventSink {
    /// Log a debug message.
    ///
    /// @param format The message format string
    /// @param args The arguments to be formatted
    void debug(String format, Object... args);
    /// Log an info message.
    ///
    /// @param format The message format string
    /// @param args The arguments to be formatted
    void info(String format, Object... args);
    /// Log a warning message.
    ///
    /// @param format The message format string
    /// @param args The arguments to be formatted
    void warn(String format, Object... args);
    /// Log a warning message with an exception.
    ///
    /// @param message The warning message
    /// @param t The throwable associated with the warning
    void warn(String message, Throwable t);
    /// Log an error message.
    ///
    /// @param format The message format string
    /// @param args The arguments to be formatted
    void error(String format, Object... args);
    /// Log an error message with an exception.
    ///
    /// @param message The error message
    /// @param t The throwable associated with the error
    void error(String message, Throwable t);
    /// Log a trace message.
    ///
    /// @param format The message format string
    /// @param args The arguments to be formatted
    void trace(String format, Object... args);
}
