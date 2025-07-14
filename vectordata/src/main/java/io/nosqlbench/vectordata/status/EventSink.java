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

// Legacy MerklePainterEvent for backward compatibility only

import java.util.Map;

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

    /// Log a message with an EventType and named parameters.
    /// The logging level is determined by the event's level.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    default void log(EventType event, Map<String, Object> params) {
        validateRequiredParams(event, params);

        switch (event.getLevel()) {
            case TRACE:
                trace(formatEventMessage(event, params));
                break;
            case DEBUG:
                debug(formatEventMessage(event, params));
                break;
            case INFO:
                info(formatEventMessage(event, params));
                break;
            case WARN:
                warn(formatEventMessage(event, params));
                break;
            case ERROR:
                error(formatEventMessage(event, params));
                break;
        }
    }

    /// Log a message with a MerklePainterEvent and named parameters.
    /// The logging level is determined by the event's level.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void log(MerklePainterEvent event, Map<String, Object> params) {
        // This method is kept for backward compatibility
        // MerklePainterEvent now implements EventType
        log((EventType) event, params);
    }

    /// Validate that all required parameters are present and of the correct type
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    default void validateRequiredParams(EventType event, Map<String, Object> params) {
        for (Map.Entry<String, Class<?>> requiredParam : event.getRequiredParams().entrySet()) {
            String paramName = requiredParam.getKey();
            Class<?> paramType = requiredParam.getValue();

            if (!params.containsKey(paramName)) {
                throw new IllegalArgumentException("Missing required parameter: " + paramName + " for event: " + event.name());
            }

            Object value = params.get(paramName);
            if (value == null) {
                continue; // Null values are allowed
            }

            Class<?> valueType = value.getClass();

            // Direct instance check
            if (paramType.isInstance(value)) {
                continue;
            }

            // Handle primitive and boxed type equivalence
            if (isPrimitiveOrBoxedEquivalent(paramType, valueType)) {
                continue;
            }

            // Check if the types are assignable or castable
            if (paramType.isAssignableFrom(valueType)) {
                continue;
            }

            // Try to see if a cast would be valid
            try {
                // For numeric types, check if they can be cast without loss of precision
                if (Number.class.isAssignableFrom(paramType) && Number.class.isAssignableFrom(valueType)) {
                    // This would allow Integer -> Long, Float -> Double, etc.
                    continue;
                }
            } catch (Exception ignored) {
                // If any exception occurs during the cast check, fall through to the error
            }

            // If we get here, the types are not compatible
            throw new IllegalArgumentException("Parameter " + paramName + " for event " + event.name() + 
                " must be of type " + paramType.getSimpleName() + ", but was " + valueType.getSimpleName());
        }
    }

    /// Check if two types are primitive/boxed equivalents of each other
    ///
    /// @param type1 First type to check
    /// @param type2 Second type to check
    /// @return true if the types are primitive/boxed equivalents
    private static boolean isPrimitiveOrBoxedEquivalent(Class<?> type1, Class<?> type2) {
        // Check primitive <-> boxed equivalence
        if (type1 == Integer.class && type2 == int.class) return true;
        if (type1 == int.class && type2 == Integer.class) return true;

        if (type1 == Long.class && type2 == long.class) return true;
        if (type1 == long.class && type2 == Long.class) return true;

        if (type1 == Double.class && type2 == double.class) return true;
        if (type1 == double.class && type2 == Double.class) return true;

        if (type1 == Float.class && type2 == float.class) return true;
        if (type1 == float.class && type2 == Float.class) return true;

        if (type1 == Boolean.class && type2 == boolean.class) return true;
        if (type1 == boolean.class && type2 == Boolean.class) return true;

        if (type1 == Character.class && type2 == char.class) return true;
        if (type1 == char.class && type2 == Character.class) return true;

        if (type1 == Byte.class && type2 == byte.class) return true;
        if (type1 == byte.class && type2 == Byte.class) return true;

        if (type1 == Short.class && type2 == short.class) return true;
        if (type1 == short.class && type2 == Short.class) return true;

        return false;
    }

    /// Validate that all required parameters are present and of the correct type
    /// (Backward compatibility method)
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use validateRequiredParams(EventType, Map) instead
    default void validateRequiredParams(MerklePainterEvent event, Map<String, Object> params) {
        validateRequiredParams((EventType) event, params);
    }

    /// Format an event message with named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @return Formatted message string
    default String formatEventMessage(EventType event, Map<String, Object> params) {
        // Include the level symbol as the first character
        StringBuilder sb = new StringBuilder();
        sb.append(event.getLevelSymbol()).append(event.name());

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                sb.append(" ").append(entry.getKey()).append(":=").append(entry.getValue());
            }
        }
        return sb.toString();
    }

    /// Format an event message with named parameters.
    /// (Backward compatibility method)
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @return Formatted message string
    /// @deprecated Use formatEventMessage(EventType, Map) instead
    default String formatEventMessage(MerklePainterEvent event, Map<String, Object> params) {
        return formatEventMessage((EventType) event, params);
    }

    /// Convenience method to log a message with an EventType and varargs parameters.
    /// The logging level is determined by the event's level.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    default void log(EventType event, Object... params) {
        log(event, paramsToMap(params));
    }

    /// Convenience method to log a message with a MerklePainterEvent and varargs parameters.
    /// The logging level is determined by the event's level.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void log(MerklePainterEvent event, Object... params) {
        log((EventType) event, params);
    }

    /// Convert varargs parameters to a map.
    ///
    /// @param params Alternating parameter names and values
    /// @return Map of parameter names to values
    default Map<String, Object> paramsToMap(Object... params) {
        if (params.length % 2 != 0) {
            throw new IllegalArgumentException("Parameters must be provided as name-value pairs");
        }

        Map<String, Object> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < params.length; i += 2) {
            if (!(params[i] instanceof String)) {
                throw new IllegalArgumentException("Parameter names must be strings");
            }
            map.put((String) params[i], params[i + 1]);
        }
        return map;
    }

    // EventType methods

    /// Convenience method to log a debug message with an EventType and named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(event, params) instead
    default void debug(EventType event, Map<String, Object> params) {
        log(event, params);
    }

    /// Convenience method to log an info message with an EventType and named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(event, params) instead
    default void info(EventType event, Map<String, Object> params) {
        log(event, params);
    }

    /// Convenience method to log a warning message with an EventType and named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(event, params) instead
    default void warn(EventType event, Map<String, Object> params) {
        log(event, params);
    }

    /// Convenience method to log an error message with an EventType and named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(event, params) instead
    default void error(EventType event, Map<String, Object> params) {
        log(event, params);
    }

    /// Convenience method to log a trace message with an EventType and named parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(event, params) instead
    default void trace(EventType event, Map<String, Object> params) {
        log(event, params);
    }

    /// Convenience method to log a debug message with an EventType and varargs parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(event, params) instead
    default void debug(EventType event, Object... params) {
        log(event, params);
    }

    /// Convenience method to log an info message with an EventType and varargs parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(event, params) instead
    default void info(EventType event, Object... params) {
        log(event, params);
    }

    /// Convenience method to log a warning message with an EventType and varargs parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(event, params) instead
    default void warn(EventType event, Object... params) {
        log(event, params);
    }

    /// Convenience method to log an error message with an EventType and varargs parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(event, params) instead
    default void error(EventType event, Object... params) {
        log(event, params);
    }

    /// Convenience method to log a trace message with an EventType and varargs parameters.
    ///
    /// @param event The EventType enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(event, params) instead
    default void trace(EventType event, Object... params) {
        log(event, params);
    }

    // Backward compatibility methods

    /// Log a debug message with a MerklePainterEvent and named parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void debug(MerklePainterEvent event, Map<String, Object> params) {
        debug((EventType) event, params);
    }

    /// Log an info message with a MerklePainterEvent and named parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void info(MerklePainterEvent event, Map<String, Object> params) {
        info((EventType) event, params);
    }

    /// Log a warning message with a MerklePainterEvent and named parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void warn(MerklePainterEvent event, Map<String, Object> params) {
        warn((EventType) event, params);
    }

    /// Log an error message with a MerklePainterEvent and named parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void error(MerklePainterEvent event, Map<String, Object> params) {
        error((EventType) event, params);
    }

    /// Log a trace message with a MerklePainterEvent and named parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Map of parameter names to values
    /// @deprecated Use log(EventType, Map) instead
    default void trace(MerklePainterEvent event, Map<String, Object> params) {
        trace((EventType) event, params);
    }

    /// Convenience method to log a debug message with a MerklePainterEvent and varargs parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void debug(MerklePainterEvent event, Object... params) {
        debug((EventType) event, params);
    }

    /// Convenience method to log an info message with a MerklePainterEvent and varargs parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void info(MerklePainterEvent event, Object... params) {
        info((EventType) event, params);
    }

    /// Convenience method to log a warning message with a MerklePainterEvent and varargs parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void warn(MerklePainterEvent event, Object... params) {
        warn((EventType) event, params);
    }

    /// Convenience method to log an error message with a MerklePainterEvent and varargs parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void error(MerklePainterEvent event, Object... params) {
        error((EventType) event, params);
    }

    /// Convenience method to log a trace message with a MerklePainterEvent and varargs parameters.
    ///
    /// @param event The MerklePainterEvent enum value
    /// @param params Alternating parameter names and values
    /// @deprecated Use log(EventType, Object...) instead
    default void trace(MerklePainterEvent event, Object... params) {
        trace((EventType) event, params);
    }
}
