/// This package is for cross-thread stateful interfaces
/// These are needed, for example, when running a logical function
/// with many threads, in which case each thread has a handle to a shared
/// context which **needs to be thread safe**.
///
/// There are two layers of aggregation which may need to be clarified:
/// 1. When many threads implementing the same function usage need to share state
/// 2. When many instances of a logical function implementation are used in the same flow
///    - There is no explicit design abstraction for this part yet
package io.nosqlbench.nbvectors.jjq.contexts;