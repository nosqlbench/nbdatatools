package io.nosqlbench.vectordata.events;

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

import java.util.Map;

/// Interface for event types that can be logged through an EventSink.
///
/// This interface defines the common methods that any event enum should provide,
/// such as getting the event's logging level and required parameters.
/// It allows EventSink to be decoupled from specific event enum implementations.
///
/// # Implementor Guidelines
///
/// When implementing this interface, follow these guidelines:
///
/// 1. Implement this interface as an enum where each enum constant represents a specific event type.
///    ```java
///    public enum MyCustomEvent implements EventType {
///        // Event constants with their levels and parameters
///    }
///    ```
///
/// 2. Use consistent naming conventions for event constants:
///    - Use uppercase for event names
///    - Consider using a prefix to categorize related events (e.g., DL_START, DL_COMPLETE)
///    - Aim for concise names (around 10-15 characters) for readability in logs
///
/// 3. Each event should have a defined logging level from EventType.Level:
///    ```java
///    MY_EVENT(EventType.Level.INFO)
///    ```
///
/// 4. For events that require parameters, define them with their types:
///    ```java
///    MY_EVENT(EventType.Level.INFO, 
///             Map.of("paramName", String.class, 
///                    "anotherParam", Long.class))
///    ```
///
/// 5. Consider including parameter descriptions for better documentation:
///    ```java
///    // Using a helper method similar to MerklePainterEvent.param()
///    MY_EVENT(EventType.Level.INFO,
///             param("paramName", String.class, "Description of this parameter"),
///             param("anotherParam", Long.class, "Description of another parameter"))
///    ```
///
/// 6. Implement the required methods:
///    - `getLevel()`: Return the logging level for the event
///    - `getRequiredParams()`: Return a map of parameter names to their types
///    - The `name()` method is provided by the enum
///
/// 7. Ensure all required parameters are validated when events are logged through an EventSink.
public interface EventType {
    /// Logging levels for events
    enum Level {
        /// Most detailed level of logging, used for fine-grained debugging information
        /// that is typically only useful during development
        TRACE,

        /// Detailed information useful for debugging purposes
        /// but not as verbose as TRACE
        DEBUG,

        /// Standard information messages about normal application operation
        INFO,

        /// Potentially harmful situations that might indicate problems
        /// but allow the application to continue running
        WARN,

        /// Error events that might still allow the application to continue running
        /// but indicate serious problems that should be addressed
        ERROR;

        /// Get the single character representation of this level
        ///
        /// @return A single character representing the level
        public char getSymbol() {
            return name().charAt(0);
        }
    }

    /// Get the effective logging level for this event
    ///
    /// @return The logging level
    Level getLevel();

    /// Get the required parameters and their types for this event
    ///
    /// @return Map of parameter names to their required types
    Map<String, Class<?>> getRequiredParams();

    /// Get the name of this event
    ///
    /// @return The event name
    String name();

    /// Get the symbolic character for the event level
    ///
    /// @return A single character representing the event level
    default char getLevelSymbol() {
        return getLevel().getSymbol();
    }
}
