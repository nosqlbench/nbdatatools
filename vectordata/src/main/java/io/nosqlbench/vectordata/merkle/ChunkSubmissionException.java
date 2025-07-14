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


/**
 * Exception thrown when a chunk submission fails.
 * This exception is used to signal errors during chunk submission operations
 * rather than using boolean return values.
 */
public class ChunkSubmissionException extends Exception {
    
    /**
     * Constructs a new ChunkSubmissionException with the specified detail message.
     *
     * @param message the detail message
     */
    public ChunkSubmissionException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new ChunkSubmissionException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public ChunkSubmissionException(String message, Throwable cause) {
        super(message, cause);
    }
}
