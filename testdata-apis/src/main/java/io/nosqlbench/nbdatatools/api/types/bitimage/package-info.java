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

/// This package contains the BitImage class, which provides an enhanced bit field representation
/// that explicitly tracks both zero and one values.
/// 
/// ## What Makes BitImage Special
/// 
/// Unlike Java's standard {@link java.util.BitSet} which only tracks one-values and determines
/// its size based on the highest set bit, BitImage explicitly tracks both zero and one values.
/// This allows for:
/// 
/// - Explicit representation of zero values (not just implied zeros)
/// - Length calculation based on the maximum index of either explicitly set zeros or ones
/// - Creation of a complete "image" of a bit field with all positions accounted for
/// - Efficient visual representation of bit patterns
/// 
/// ## BitImage Structure
/// 
/// ```
/// ┌───────────────────────────────────────────────┐
/// │                   BitImage                    │
/// ├───────────────────────────────────────────────┤
/// │                                               │
/// │  ┌─────────────┐           ┌─────────────┐   │
/// │  │  ones BitSet│           │ zeros BitSet│   │
/// │  ├─────────────┤           ├─────────────┤   │
/// │  │ Tracks bits │           │ Tracks bits │   │
/// │  │ set to 1    │           │ set to 0    │   │
/// │  └─────────────┘           └─────────────┘   │
/// │                                               │
/// │  length() = max(ones.length(), zeros.length())│
/// │                                               │
/// └───────────────────────────────────────────────┘
/// ```
/// 
/// ## Comparison with Standard BitSet
/// 
/// ```
/// ┌─────────────────────────┬─────────────────────────┐
/// │        BitSet           │        BitImage         │
/// ├─────────────────────────┼─────────────────────────┤
/// │ Only tracks 1-bits      │ Tracks both 0 and 1 bits│
/// │                         │                         │
/// │ Size based on highest   │ Size based on highest   │
/// │ set 1-bit               │ explicitly set bit      │
/// │                         │ (0 or 1)                │
/// │                         │                         │
/// │ Zeros are implied       │ Zeros are explicitly    │
/// │                         │ tracked                 │
/// │                         │                         │
/// │ Cannot distinguish      │ Can distinguish between │
/// │ between unset bits and  │ unset bits and bits     │
/// │ bits set to 0           │ explicitly set to 0     │
/// └─────────────────────────┴─────────────────────────┘
/// ```
/// 
/// ## Visual Representation
/// 
/// BitImage is particularly useful for creating visual representations of bit patterns,
/// especially when combined with braille characters for compact display:
/// 
/// ```
/// BitImage Example:
/// 
/// Index:  0 1 2 3 4 5 6 7
/// Values: 1 0 1 1 0 0 1 0
/// 
/// ones BitSet:  {0, 2, 3, 6}
/// zeros BitSet: {1, 4, 5, 7}
/// 
/// Braille representation: ⢕
/// 
/// Bit positions in braille:
/// 1 4  →  ● ○
/// 2 5  →  ○ ○
/// 3 6  →  ● ●
/// 7 8  →  ○ ●
/// ```
/// 
/// ## Common Use Cases
/// 
/// ```
/// ┌─────────────────────────────────────────────────────────┐
/// │ 1. Matching Patterns Between Arrays                     │
/// ├─────────────────────────────────────────────────────────┤
/// │ BitImage can represent which elements from an expected  │
/// │ array are present in a provided array, with explicit    │
/// │ tracking of both matches (1) and mismatches (0).        │
/// └─────────────────────────────────────────────────────────┘
/// 
/// ┌─────────────────────────────────────────────────────────┐
/// │ 2. Visual Representation of Bit Patterns                │
/// ├─────────────────────────────────────────────────────────┤
/// │ BitImage can be converted to braille characters for     │
/// │ compact visual representation of bit patterns, making   │
/// │ it easier to understand complex bit fields at a glance. │
/// └─────────────────────────────────────────────────────────┘
/// 
/// ┌─────────────────────────────────────────────────────────┐
/// │ 3. Complete Bit Field Representation                    │
/// ├─────────────────────────────────────────────────────────┤
/// │ When you need to track both explicitly set zeros and    │
/// │ ones in a bit field, with accurate length calculation.  │
/// └─────────────────────────────────────────────────────────┘
/// ```
/// 
/// ## Example Usage Flow
/// 
/// ```
/// ┌───────────┐     ┌───────────┐     ┌───────────┐
/// │ Create    │     │ Set bits  │     │ Get mask  │
/// │ BitImage  │ ──▶ │ explicitly│ ──▶ │ or length │
/// │           │     │ to 0 or 1 │     │           │
/// └───────────┘     └───────────┘     └───────────┘
///       │                                    │
///       │                                    ▼
///       │                            ┌───────────┐
///       │                            │ Convert to│
///       └───────────────────────────▶│ visual    │
///                                    │ format    │
///                                    └───────────┘
///
/// ---
/// A wrapper type BitSetTracker can be used to do the following:
/// * It is a direct API wrapper around BitSet, extending BitSet to it can be dropped in to
/// replace BitSet without other code changes.
/// * When a BitSetTracker is created, it requires a path to a file with a '.bimg' extension.
/// * If the file is already present it will be replaced. It will be created otherwise.
/// * The file is memory mapped to be the right size for a backing store for unicode characters,
///  with the help of a bytebuffer and charbuffer.
/// * The initial text image of the charbuffer is set to the braille picture of the bitimage.
/// * When a bit is set or flipped in the underlying bitimage via the wrapped API, the braille
/// character is modified in place.
/// * BitSetTracker info is considered ephemeral and secondary to the contents of a BitSet, i.e,
///  no BitSet should ever be populated from the contents of a bimg file.
/// * BitSetTracker should maintain a bytebuffer which holes actual 3-byte unicode character
/// encodings. This allows for indexed updates to individual characters.
/// * When the byte buffer for unicode characters is created or resized, it should always have
/// the "zero dots" unicode character filled in the new 3-byte positions.
/// * The canonical representation of the character image should be the 3-byte byte buffer version.
///   * All other views of character data must be derived from this dynamically.
///   * A method which gets the Java char equivalent of a given position should be provided.
///   * A method which gets the Java String equivalent of the whole buffer should be provided.
///   * Otherwise no string form of the character buffer should be maintained apart from the
/// actual BitSet from which it is derived.
///
///
///
/// ```
package io.nosqlbench.nbdatatools.api.types.bitimage;
