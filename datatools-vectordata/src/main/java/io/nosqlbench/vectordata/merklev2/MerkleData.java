package io.nosqlbench.vectordata.merklev2;

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


/// This is an interface to a merkle tree which provides access to the hash data and the shape of the tree.
/// It can be used for either a reference tree or a state tree. This is done by aliasing the type
///  by extension, and declaring any instance of it as a ref or a state. The distinct semantics
/// for each of these two types are defined by the two sub-interfaces, and the distinctly named
/// methods thusly.
///
/// Implementation requirements:
/// * The data structures used should be mapped onto underlying byte arrays as part of a memory
/// mapped file. This consists of three regions:
/// 1. The hash data region
/// 2. The valid bitset region
/// 3. The footer region
/// A MerkleShape should be used to define the layout in terms of data sizes, and all MerkleShape
///  fields should be persisted in the footer.
///
/// All IO should be done with absolute positioning to avoid race conditions.
/// Thread safety should be provided where appropriate with locks or memory barriers.
public interface MerkleData extends MerkleRef, MerkleState, AutoCloseable {
}
