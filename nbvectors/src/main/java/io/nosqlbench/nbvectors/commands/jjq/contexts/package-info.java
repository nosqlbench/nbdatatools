/// state contexts for use with the {@link io.nosqlbench.nbvectors.commands.jjq.CMD_jjq} command
///
/// This package is for cross-thread stateful interfaces
/// These are needed, for example, when running a logical function
/// with many threads, in which case each thread has a handle to a shared
/// context which **needs to be thread safe**.
///
/// There are two layers of aggregation which may need to be clarified:
/// 1. When many threads implementing the same function usage need to share state
/// 2. When many instances of a logical function implementation are used in the same flow
///    - There is no explicit design abstraction for this part yet
package io.nosqlbench.nbvectors.commands.jjq.contexts;

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

