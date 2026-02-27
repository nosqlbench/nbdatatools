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

package io.nosqlbench.slabtastic;

/// A single request within a multi-batch read operation.
///
/// Each request identifies a namespace and an ordinal to look up. The
/// default namespace (empty string) is used when only an ordinal is
/// specified.
///
/// @param namespace the namespace to read from
/// @param ordinal   the global ordinal to look up
public record BatchRequest(String namespace, long ordinal) {

    /// Creates a request for the default namespace.
    ///
    /// @param ordinal the global ordinal to look up
    /// @return a request targeting the default namespace
    public static BatchRequest of(long ordinal) {
        return new BatchRequest("", ordinal);
    }

    /// Creates a request for a specific namespace.
    ///
    /// @param namespace the namespace to read from
    /// @param ordinal   the global ordinal to look up
    /// @return a request targeting the given namespace and ordinal
    public static BatchRequest of(String namespace, long ordinal) {
        return new BatchRequest(namespace, ordinal);
    }
}
