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

import io.nosqlbench.vectordata.status.EventType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/// Enum representing all loggable events for a MerklePainter.
///
/// Each event has a mnemonic in uppercase, justified to no more than 15 characters wide. 
/// These mnemonics are used when logging details through the event sink. 
/// Each event also has an effective logging level and a list of required parameters with their types.
public enum MerklePainterEvent implements EventType {
    // Download related events
    /// Download start event
    DOWNLOAD_START   (EventType.Level.INFO),

    /// Download complete event
    DOWNLOAD_COMPLETE(EventType.Level.INFO),

    /// Download failed event
    DOWNLOAD_FAILED  (EventType.Level.ERROR, 
                     param("index", Long.class, "The index of the chunk that failed to download"),
                     param("start", Long.class, "The starting byte position of the chunk"),
                     param("size", Long.class, "The size in bytes of the chunk"),
                     param("reason", String.class, "The reason for the download failure")),

    // Chunk related events
    /// Chunk submission start event
    CHUNK_START      (EventType.Level.INFO, param("index", Long.class, "The index of the chunk being submitted")),

    /// Chunk leaf invalidated event
    CHUNK_INVALID    (EventType.Level.DEBUG, param("index", Long.class, "The index of the chunk with invalidated leaf")),

    /// Chunk hash zeroed event
    CHUNK_ZEROED     (EventType.Level.DEBUG, param("index", Long.class, "The index of the chunk with zeroed hash")),

    /// Chunk valid bit set event
    CHUNK_VALID      (EventType.Level.INFO, param("index", Long.class, "The index of the chunk that was validated")),

    /// Chunk submission failed event
    CHUNK_FAILED     (EventType.Level.ERROR, 
                     param("index", Long.class, "The index of the chunk that failed submission"), 
                     param("text", String.class, "Error message describing the failure")),

    /// Chunk processing error event
    CHUNK_PROC_ERROR (EventType.Level.ERROR, 
                     param("index", Long.class, "The index of the chunk with processing error"), 
                     param("text", String.class, "Error message describing the processing error")),

    // Range related events
    /// Range download start event
    RANGE_START      (EventType.Level.INFO, 
                     tupleStart("from", "to", Long.class, "The starting chunk index of the range"),
                     tupleEnd("to", Long.class, "The ending chunk index of the range"),
                     param("begin", Long.class, "The starting byte position of the range"),
                     param("end", Long.class, "The ending byte position of the range"),
                     param("size", Long.class, "The total size in bytes of the range")),

    /// Range download complete event
    RANGE_COMPLETE   (EventType.Level.INFO, 
                     tupleStart("from", "to", Long.class, "The starting chunk index of the range"),
                     tupleEnd("to", Long.class, "The ending chunk index of the range"),
                     param("begin", Long.class, "The starting byte position of the range"),
                     param("end", Long.class, "The ending byte position of the range"),
                     param("size", Long.class, "The total size in bytes of the range")),

    /// Range download failed event
    RANGE_FAILED     (EventType.Level.ERROR, 
                     tupleStart("from", "to", Long.class, "The starting chunk index of the range"),
                     tupleEnd("to", Long.class, "The ending chunk index of the range"),
                     param("begin", Long.class, "The starting byte position of the range"),
                     param("end", Long.class, "The ending byte position of the range"),
                     param("size", Long.class, "The total size in bytes of the range")),

    /// Auto-buffer mode enabled event
    AUTO_BUFFER_ON   (EventType.Level.INFO,
                     param("count", Long.class, "The number of contiguous requests"),
                     param("threshold", Long.class, "The threshold for enabling auto-buffer mode")),

    /// Read-ahead download started event
    READ_AHEAD       (EventType.Level.INFO,
                     param("from", Long.class, "The starting chunk index of the read-ahead range"),
                     param("to", Long.class, "The ending chunk index of the read-ahead range")),

    // Shutdown related events
    /// Shutdown initiated event
    SHUTDOWN_INIT    (EventType.Level.INFO, param("path", String.class, "The path of the file being shut down")),

    /// Stopping transfers during shutdown event
    SHUTDOWN_STOPPING(EventType.Level.INFO),

    /// Computing hashes during shutdown event
    SHUTDOWN_HASHING (EventType.Level.INFO),

    /// Flushing data during shutdown event
    SHUTDOWN_FLUSHING(EventType.Level.INFO),

    /// Shutdown complete event
    SHUTDOWN_COMPLETE(EventType.Level.INFO, param("path", String.class, "The path of the file that was shut down")),

    // Error related events
    /// Error during download event
    ERROR_DOWNLOAD   (EventType.Level.ERROR, 
                     param("start", Long.class, "The starting position of the download"),
                     param("size", Long.class, "The size of the download in bytes"),
                     param("code", Integer.class, "The HTTP response code (if applicable)"),
                     param("text", String.class, "Error message describing the download error")),

    /// Error during painting event
    ERROR_PAINTING   (EventType.Level.ERROR, param("text", String.class, "Error message describing the painting error")),

    /// Error with chunk event
    ERROR_CHUNK      (EventType.Level.ERROR, 
                     param("index", Long.class, "The index of the chunk with error"), 
                     param("text", String.class, "Error message describing the chunk error")),

    /// Error during read-ahead event
    ERROR_READ_AHEAD (EventType.Level.ERROR,
                     param("from", Long.class, "The starting chunk index of the read-ahead range"),
                     param("to", Long.class, "The ending chunk index of the read-ahead range"),
                     param("text", String.class, "Error message describing the read-ahead error")),

    /// Waiting for existing downloads to complete
    WAITING_DOWNLOADS(EventType.Level.INFO,
                     param("from", Long.class, "The starting chunk index of the range being waited for"),
                     param("to", Long.class, "The ending chunk index of the range being waited for")),

    /// Error while waiting for downloads to complete
    ERROR_WAITING    (EventType.Level.ERROR,
                     param("from", Long.class, "The starting chunk index of the range being waited for"),
                     param("to", Long.class, "The ending chunk index of the range being waited for"),
                     param("text", String.class, "Error message describing the waiting error")),

    /// Error with file event
    ERROR_FILE       (EventType.Level.ERROR, param("text", String.class, "Error message describing the file error")),

    /// Error computing hash event
    ERROR_HASH       (EventType.Level.ERROR, param("text", String.class, "Error message describing the hash computation error")),

    /// Error saving merkle tree event
    ERROR_SAVE       (EventType.Level.ERROR, param("text", String.class, "Error message describing the save error")),

    /// Error closing pane event
    ERROR_CLOSE      (EventType.Level.ERROR, param("text", String.class, "Error message describing the close error")),

    /// General error event
    ERROR_GENERAL    (EventType.Level.ERROR, param("text", String.class, "Error message describing the general error")),

    /// Chunk verification start event
    CHUNK_VFY_START(EventType.Level.INFO, param("index", Long.class, "The index of the chunk being verified")),

    /// Chunk verification success event
    CHUNK_VFY_OK   (EventType.Level.INFO, param("index", Long.class, "The index of the chunk that was verified successfully")),

    /// Chunk verification failed event
    CHUNK_VFY_FAIL (EventType.Level.ERROR, 
                     param("index", Long.class, "The index of the chunk that failed verification"), 
                     param("text", String.class, "Error message describing the verification failure"),
                     param("refHash", String.class, "The reference hash value in hexadecimal"),
                     param("compHash", String.class, "The computed hash value in hexadecimal")),

    /// Chunk verification retry event
    CHUNK_VFY_RETRY(EventType.Level.INFO, 
                     param("index", Long.class, "The index of the chunk being retried for verification"), 
                     param("attempt", Integer.class, "The current retry attempt number"));


    /// Record representing a parameter with name, type, and description
    private record ParamInfo(String name, Class<?> type, String description, boolean isTupleStart, String tupleEnd) {
        private ParamInfo {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
            Objects.requireNonNull(description, "Parameter description cannot be null");
            // tupleEnd can be null if this is not a tuple start parameter
        }

        /// Check if this parameter is part of a tuple
        public boolean isTuple() {
            return isTupleStart || tupleEnd != null;
        }

        /// Get the tuple name (derived from the parameter name)
        public String getTupleName() {
            if (!isTupleStart) {
                return null;
            }
            // Extract the base name without "Start" or "End" suffix
            String baseName = name;
            if (name.endsWith("Start")) {
                baseName = name.substring(0, name.length() - 5);
            } else if (name.contains("start")) {
                baseName = name.replace("start", "");
            }
            return baseName;
        }
    }

    private final EventType.Level level;
    private final Map<String, Class<?>> requiredParams;
    private final Map<String, String> paramDescriptions;

    /// Constructor for MerklePainterEvent with a logging level and required parameters
    ///
    /// @param level The effective logging level for this event
    /// @param requiredParams Array of parameter information including name, type, and description
    MerklePainterEvent(EventType.Level level, ParamInfo... requiredParams) {
        this.level = level;
        Map<String, Class<?>> params = new LinkedHashMap<>();
        Map<String, String> descriptions = new LinkedHashMap<>();
        for (ParamInfo info : requiredParams) {
            params.put(info.name(), info.type());
            descriptions.put(info.name(), info.description());
        }
        this.requiredParams = Collections.unmodifiableMap(params);
        this.paramDescriptions = Collections.unmodifiableMap(descriptions);
    }

    /// Get the effective logging level for this event
    ///
    /// @return The logging level
    @Override
    public EventType.Level getLevel() {
        return level;
    }

    /// Get the required parameters and their types for this event
    ///
    /// @return Map of parameter names to their required types
    @Override
    public Map<String, Class<?>> getRequiredParams() {
        return requiredParams;
    }

    /// Get the descriptions for all parameters of this event
    ///
    /// @return Map of parameter names to their descriptions
    public Map<String, String> getParamDescriptions() {
        return paramDescriptions;
    }

    /// Helper method to create a parameter with name, type, and description
    ///
    /// @param name The parameter name
    /// @param type The parameter type
    /// @param description The parameter description
    /// @return A ParamInfo object with the parameter details
    private static ParamInfo param(String name, Class<?> type, String description) {
        return new ParamInfo(name, type, description, false, null);
    }

    /// Helper method to create a parameter with name and type (for backward compatibility)
    ///
    /// @param name The parameter name
    /// @param type The parameter type
    /// @return A ParamInfo object with the parameter details and empty description
    private static ParamInfo param(String name, Class<?> type) {
        return new ParamInfo(name, type, "", false, null);
    }

    /// Helper method to create a start tuple parameter
    ///
    /// @param startName The name for the start parameter
    /// @param endName The name for the end parameter
    /// @param type The parameter type for the start parameter
    /// @param description The description for the start parameter
    /// @return A ParamInfo object representing the start of a tuple
    private static ParamInfo tupleStart(String startName, String endName, Class<?> type, String description) {
        return new ParamInfo(startName, type, description, true, endName);
    }

    /// Helper method to create an end tuple parameter
    ///
    /// @param endName The name for the end parameter
    /// @param type The parameter type for the end parameter
    /// @param description The description for the end parameter
    /// @return A ParamInfo object representing the end of a tuple
    private static ParamInfo tupleEnd(String endName, Class<?> type, String description) {
        return new ParamInfo(endName, type, description, false, null);
    }

    /// Helper method to create a tuple parameter with start and end names
    ///
    /// @param tupleName The base name for the tuple (e.g., "chunk" for "chunk(start,end)")
    /// @param startName The name for the start parameter
    /// @param endName The name for the end parameter
    /// @param type The parameter type for both start and end
    /// @param startDescription The description for the start parameter
    /// @param endDescription The description for the end parameter
    /// @return An array of ParamInfo objects representing the tuple
    private static ParamInfo[] tuple(String tupleName, String startName, String endName, Class<?> type, 
                                    String startDescription, String endDescription) {
        return new ParamInfo[] {
            new ParamInfo(startName, type, startDescription, true, endName),
            new ParamInfo(endName, type, endDescription, false, null)
        };
    }
}
