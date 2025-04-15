package io.nosqlbench.vectordata.download.merkle;

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


/// Defines the download span mode for Merkle tree-based downloads.
///
/// This enum specifies whether to download the full file or a specific portion
/// up to a target size.
///
public enum SpanMode {
    /// Use the full span available in the merkle tree
    FULL_SPAN,

    /// Use a specific target span size
    TARGET_SPAN
}
