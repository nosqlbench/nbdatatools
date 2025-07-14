# Merkle Tree File Format Documentation

## Overview
This document explains the structure of merkle tree files (`.mrkl` files) in the NoSQLBench vectordata module, with a specific focus on where the root merkle hash is located.

## File Structure
The `.mrkl` file format consists of several sections stored sequentially:

1. **Leaf Hashes**: The hashes of the leaf nodes (data chunks)
2. **Padded Leaves**: Zero-filled hashes for padding if needed
3. **Internal Nodes**: The hashes of the internal nodes, including the root hash
4. **BitSet Data**: A BitSet tracking which nodes are valid
5. **Footer**: Metadata about the merkle tree (chunk size, total size, BitSet size)

## Root Merkle Hash Location
The root merkle hash is stored as the first hash in the internal nodes section of the file. In the MerkleTree implementation, the root node is always at index 0.

When the merkle tree is saved to a file (see `MerkleTree.save(Path path)`), the internal nodes are written after the leaf nodes. The root hash (at index 0) is the first hash in the internal nodes section.

### Accessing the Root Hash
To access the root hash from a merkle tree file:

1. Load the merkle tree using `MerkleTree.load(Path path)`
2. Call `getHash(0)` on the loaded tree to get the root hash

The `getHash(0)` method will either:
- Return the cached hash if available
- Read the hash from the memory-mapped buffer if the node is valid
- Compute the hash from its children if necessary

## File Format Details
The exact layout of the file depends on the tree structure:

1. **Padded Tree Format**: Contains all nodes (leaves and internal nodes) with padding
2. **Exact Complete Tree Format**: Contains all nodes without padding
3. **Leaves-Only Format**: Contains only leaf nodes
4. **Padded-Leaves-Only Format**: Contains only leaf nodes with padding

In all formats, the root hash is either:
- Stored explicitly in the file (formats 1 and 2)
- Computed from the leaf nodes when needed (formats 3 and 4)

## Memory-Mapped Access
The merkle tree implementation uses memory-mapped file access for efficient random access to the hash data. This allows direct reading and writing of hashes without loading the entire file into memory.

When a hash is needed, the implementation first checks if it can be read directly from the memory-mapped buffer. If not, it computes the hash from its children.