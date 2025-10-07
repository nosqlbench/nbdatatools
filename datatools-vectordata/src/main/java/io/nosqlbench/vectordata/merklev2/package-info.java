/// This contains a v2 implementation of the merkle tree to fix issues without using existing code.
///
/// The goal of this package is to provide a more robust and correct implementation of the merkle
/// tree logic. The existing implementation in the merkle package has some issues which are
/// challenging to fix without a ground-up rewrite. This package is intended to be a replacement
/// for the existing implementation.
///
/// Here is how the merkle tree logic in this package is supposed to work.
///
/// There are two main interfaces:
/// * MerkleRef - This is the interface for a merkle tree which is used as a read-only reference
/// for data integrity. It is used to verify data which has been downloaded from a remote source.
///  Merkle ref files always end in `.mref`
/// * MerkleState - This is the interface for a merkle tree which is used to track the integrity
///  of data which is being fetched from a remote source. MerkleState files are always seeded by
/// an already-computed merkle ref file, but then it is used to track the integrity of the data
/// as it is fetched. MerkleState files always end in `.mrkl`
///
/// Both of these should be defined as interfaces. They can both use the same underlying hash
/// tree implementation, although the semantics between the two interfaces are important. What is
/// important here is how the methods of the interfaces and their unique semantics map onto the
/// usage of the merkle state underneath. For the MerkleRef, the hashes are always valid. For the
/// MerkleState, the hashes are only valid for the leaves which have been fetched and verified.
///
/// When using the MerkleRef and MerkleState together, there is duplicitous data, as the bits in
/// the merkle ref are always valid. In both cases, the hash values are always valid too. This means
/// that a merkle state file can be constituted solely from a merkle tree file without having to
/// retain the merkle tree file. For this reason, when a merkle state is created from a merkle ref,
/// the merkle ref file can be discarded.
///
/// There are also helper interfaces:
/// * MerkleShape - This is the interface for describing the shape of a merkle tree. It is used
/// by all implementations to understand the layout of the tree.
/// * ChunkedTransport - This is the interface for fetching data from a remote source. It is used
///  when data needs to be fetched which is missing. The transport for the data is pluggable, and
///  should be instantiated from the factory
/// [io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO#create(java.lang.String)]
///
/// The details for each of these are in their respective javadocs.
///
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

