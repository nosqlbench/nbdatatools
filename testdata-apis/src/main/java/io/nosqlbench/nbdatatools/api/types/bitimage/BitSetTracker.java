package io.nosqlbench.nbdatatools.api.types.bitimage;

/// Copyright (c) nosqlbench
/// 
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// 
///   http://www.apache.org/licenses/LICENSE-2.0
/// 
/// Unless required by applicable law or agreed to in writing,
/// software distributed under the License is distributed on an
/// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
/// KIND, either express or implied.  See the License for the
/// specific language governing permissions and limitations
/// under the License.

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.BitSet;

/// A BitSet that tracks changes to itself and updates either a memory-mapped file
/// or an in-memory buffer with the braille representation of the BitSet.
/// 
/// This class extends BitSet and additionally updates either a memory-mapped file
/// or an in-memory buffer with the braille representation of the BitSet whenever a bit is set or flipped.
public class BitSetTracker extends BitSet {
    /// The ByteBuffer that holds the UTF-8 encoded braille characters.
    /// This can be either a memory-mapped file buffer or an in-memory buffer.
    private ByteBuffer byteBuffer;
//    private CharBuffer charBuffer;

    /// The file that this BitSetTracker is associated with.
    /// This is null if the BitSetTracker is not backed by a file.
    private final File file;

    /// Flag indicating whether this BitSetTracker is backed by a file.
    /// If true, the byteBuffer is a memory-mapped file buffer.
    /// If false, the byteBuffer is an in-memory buffer.
    private final boolean useFile;

    /// Tracks the length of content set via setFileContent method.
    /// This is used to determine how many characters to return when getBrailleData is called.
    private int contentLength;

    // Private helper method to initialize the BitSetTracker
    private void initialize(BitSet bitSet, int nodeCount) {
        try {
            // Calculate the size needed for the file based on the BitSet length or nodeCount
            // Each braille character is 3 bytes in UTF-8
            // Add extra space to ensure buffer is large enough for any future operations
            int numChars;
            if (nodeCount > 0) {
                // If nodeCount is provided, use it to determine the size
                numChars = Math.max(32, (nodeCount + 7) / 8); // 8 bits per braille character, rounded up, minimum 32 characters
            } else {
                // Otherwise, use the BitSet length
                numChars = Math.max(32, (this.length() + 7) / 8); // 8 bits per braille character, rounded up, minimum 32 characters
            }
            int bufferSize = numChars * 3; // 3 bytes per char in UTF-8

            if (useFile) {
                // Create or replace the file
                if (file.exists()) {
                    file.delete();
                }
                file.createNewFile();

                // Memory map the file
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                     FileChannel channel = randomAccessFile.getChannel()) {
                    byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
                }
            } else {
                // Create a ByteBuffer for in-memory operation
                byteBuffer = ByteBuffer.allocate(bufferSize);
            }

            // Always initialize with 'zero value' braille tiles first
            char emptyBraille = (char) Glyphs.BRAILLE_BASE;
            for (int i = 0; i < numChars; i++) {
                encodeCharTo3ByteUTF8(emptyBraille, i);
            }

            // Copy all bits from the provided BitSet to this BitSet
            if (bitSet != null) {
                this.or(bitSet);
            }

            // Then, if the BitSet is not empty, update the braille representation
            if (!this.isEmpty()) {
                String braille = Glyphs.braille(this);
                for (int i = 0; i < braille.length() && i < numChars; i++) {
                    encodeCharTo3ByteUTF8(braille.charAt(i), i);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Helper method to validate file path
    private static void validateFilePath(String filePath) {
        if (!filePath.endsWith(".bimg")) {
            throw new IllegalArgumentException("File path must have .bimg extension");
        }
    }

    /// Create a BitSetTracker with the specified BitSet and node count
    /// @param filePath path to the file to memory map (must have .bimg extension)
    /// @param bitSet the BitSet to track
    /// @param nodeCount the number of nodes in the merkle tree, used to determine the size of the BitImage
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(String filePath, BitSet bitSet, int nodeCount) {
        super();
        validateFilePath(filePath);
        this.file = new File(filePath);
        this.useFile = true;
        this.contentLength = 0;
        initialize(bitSet, nodeCount);
    }

    /// Create a BitSetTracker with the specified BitSet and node count
    /// @param path path to the file to memory map (must have .bimg extension)
    /// @param bitSet the BitSet to track
    /// @param nodeCount the number of nodes in the merkle tree, used to determine the size of the BitImage
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(Path path, BitSet bitSet, int nodeCount) {
        this(path.toString(), bitSet, nodeCount);
    }

    /// Create a BitSetTracker with the specified BitSet and node count without a file backing
    /// @param bitSet the BitSet to track
    /// @param nodeCount the number of nodes in the merkle tree, used to determine the size of the BitImage
    public BitSetTracker(BitSet bitSet, int nodeCount) {
        super();
        this.file = null;
        this.useFile = false;
        this.contentLength = 0;
        initialize(bitSet, nodeCount);
    }

    /// Create a BitSetTracker with a new BitSet of the specified length
    /// @param filepath path to the file to memory map (must have .bimg extension)
    /// @param len the length of the BitSet
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(Path filepath, int len) {
        this(filepath.toString(), new BitSet(len), len);
    }

    /// Create a BitSetTracker with a new empty BitSet
    /// @param filepath path to the file to memory map (must have .bimg extension)
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(Path filepath) {
        this(filepath.toString(), new BitSet(), 0);
    }

    /// Create a BitSetTracker with a new BitSet of the specified length
    /// @param filePath path to the file to memory map (must have .bimg extension)
    /// @param len the length of the BitSet
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(String filePath, int len) {
        this(filePath, new BitSet(len), len);
    }

    /// Create a BitSetTracker with a new empty BitSet
    /// @param filePath path to the file to memory map (must have .bimg extension)
    /// @throws IllegalArgumentException if the file path does not have .bimg extension
    public BitSetTracker(String filePath) {
        this(filePath, new BitSet(), 0);
    }

    /// Encode a single character into exactly 3 bytes in UTF-8 format
    /// @param c the character to encode
    /// @param index the index in the byte buffer where the character should be stored
    private void encodeCharTo3ByteUTF8(char c, int index) {
        // Calculate the byte position in the buffer (3 bytes per character)
        int bytePos = index * 3;

        // Ensure we don't exceed the buffer capacity
        if (bytePos + 2 >= byteBuffer.capacity()) {
            expandBuffer(index + 1);
            bytePos = index * 3; // Recalculate after expansion
        }

        // For braille characters (U+2800 to U+28FF), the UTF-8 encoding is:
        // 1110xxxx 10xxxxxx 10xxxxxx
        // where the x's are the bits of the character

        // First byte: 1110 + 4 high bits of the character
        byteBuffer.put(bytePos, (byte) (0xE0 | ((c >> 12) & 0x0F)));

        // Second byte: 10 + 6 middle bits of the character
        byteBuffer.put(bytePos + 1, (byte) (0x80 | ((c >> 6) & 0x3F)));

        // Third byte: 10 + 6 low bits of the character
        byteBuffer.put(bytePos + 2, (byte) (0x80 | (c & 0x3F)));

        // Ensure data is flushed when written to the memory-mapped region
        if (byteBuffer instanceof MappedByteBuffer) {
            ((MappedByteBuffer) byteBuffer).force();
        }
    }

    /// Decode a 3-byte UTF-8 encoded character from the byte buffer
    /// @param index the index of the character to decode
    /// @return the decoded character
    private char decode3ByteUTF8(int index) {
        // Calculate the byte position in the buffer (3 bytes per character)
        int bytePos = index * 3;

        // Ensure we don't exceed the buffer capacity
        if (bytePos + 2 >= byteBuffer.capacity()) {
            return (char) Glyphs.BRAILLE_BASE; // Return empty braille character if out of bounds
        }

        // Get the 3 bytes
        byte b1 = byteBuffer.get(bytePos);
        byte b2 = byteBuffer.get(bytePos + 1);
        byte b3 = byteBuffer.get(bytePos + 2);

        // Decode the character
        // First byte: extract the 4 bits after the 1110 prefix
        // Second byte: extract the 6 bits after the 10 prefix
        // Third byte: extract the 6 bits after the 10 prefix
        return (char) (((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
    }

    /// Expand the buffer to accommodate at least the specified number of characters
    /// @param minCharCapacity the minimum number of characters the buffer should be able to hold
    private synchronized void expandBuffer(int minCharCapacity) {
        try {
            // Calculate the new buffer size
            // Double the current capacity or use minCharCapacity, whichever is larger
            int currentCapacity = byteBuffer.capacity() / 3; // Current capacity in characters
            int newCapacity = Math.max(minCharCapacity, currentCapacity * 2);
            int newBufferSize = newCapacity * 3; // 3 bytes per character in UTF-8

            // Create a temporary copy of the current buffer content
            byte[] currentContent = new byte[byteBuffer.capacity()];
            byteBuffer.position(0);
            byteBuffer.get(currentContent);

            if (useFile) {
                // Resize the file
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                     FileChannel channel = randomAccessFile.getChannel()) {
                    randomAccessFile.setLength(newBufferSize);

                    // Map the new file size
                    MappedByteBuffer newByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, newBufferSize);

                    // Copy the old content to the new buffer
                    newByteBuffer.put(currentContent);

                    // Update the buffer references
                    byteBuffer = newByteBuffer;
//                    charBuffer = java.nio.charset.StandardCharsets.UTF_8.decode(byteBuffer);
                }
            } else {
                // Create a new ByteBuffer with the new size
                ByteBuffer newByteBuffer = ByteBuffer.allocate(newBufferSize);

                // Copy the old content to the new buffer
                newByteBuffer.put(currentContent);

                // Update the buffer references
                byteBuffer = newByteBuffer;
//                charBuffer = java.nio.charset.StandardCharsets.UTF_8.decode(byteBuffer);
            }

            // Fill the new positions with empty braille characters
            char emptyBraille = (char) Glyphs.BRAILLE_BASE;
            for (int i = currentCapacity; i < newCapacity; i++) {
                encodeCharTo3ByteUTF8(emptyBraille, i);
            }
        } catch (Exception e) {
            // If any exception occurs, log it and continue with the current buffer
            System.err.println("Error expanding buffer: " + e.getMessage());
        }
    }

    /// Update a specific braille character in the memory-mapped file or in-memory buffer
    /// @param bitIndex the index of the bit that was changed
    private synchronized void updateBrailleCharacter(int bitIndex) {
        // Reset contentLength since we're updating based on the BitSet now
        this.contentLength = 0;

        // Calculate which braille character needs to be updated
        int charIndex = bitIndex / 8;

        // If the character index is out of bounds, expand the buffer
        if (charIndex * 3 + 2 >= byteBuffer.capacity()) {
            expandBuffer(charIndex + 1); // Ensure we have enough space for this character
        }

        // Calculate which bit within the braille character needs to be updated
        int bitPosition = bitIndex % 8;

        // Get the mask for this bit position using the Glyphs.maskByShift array
        // This ensures we use the same bit mapping as the Glyphs class
        int mask = Glyphs.maskByShift[bitPosition];

        try {
            // Get the current braille character
            char currentChar = decode3ByteUTF8(charIndex);

            // If the character is not a braille character, initialize it to the empty braille character
            if (currentChar < Glyphs.BRAILLE_BASE || currentChar > Glyphs.BRAILLE_BASE + 0xFF) {
                currentChar = (char) Glyphs.BRAILLE_BASE;
            }

            // Calculate the new braille character based on the bit value using Glyphs helper methods
            char newChar;
            if (this.get(bitIndex)) {
                // Set the bit using Glyphs.orMask
                newChar = Glyphs.orMask(currentChar, mask);
            } else {
                // Clear the bit using Glyphs.andNotMask
                newChar = Glyphs.andNotMask(currentChar, mask);
            }

            // Update the character in the buffer using our custom encoding method
            encodeCharTo3ByteUTF8(newChar, charIndex);
        } catch (Exception e) {
            // If any exception occurs, just log it and return without modifying the buffer
            System.err.println("Error updating braille character at index " + charIndex + ": " + e.getMessage());
        }
    }

    /// Set the bit at the specified index to true and update the braille representation
    /// @param bitIndex the index of the bit to set
    @Override
    public synchronized void set(int bitIndex) {
        super.set(bitIndex);
        updateBrailleCharacter(bitIndex);
    }

    /// Set the bit at the specified index to true and update the braille representation
    /// @param path the path to the file to update
    /// @param bitIndex the index of the bit to set
    public static synchronized void set(Path path, int bitIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.set(bitIndex);
    }

    /// Set the bit at the specified index to the specified value and update the braille representation
    /// @param bitIndex the index of the bit to set
    /// @param value the value to set the bit to
    @Override
    public synchronized void set(int bitIndex, boolean value) {
        super.set(bitIndex, value);
        updateBrailleCharacter(bitIndex);
    }

    /// Set the bit at the specified index to the specified value and update the braille representation
    /// @param path the path to the file to update
    /// @param bitIndex the index of the bit to set
    /// @param value the value to set the bit to
    public static synchronized void set(Path path, int bitIndex, boolean value) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.set(bitIndex, value);
    }

    /// Set the bits from fromIndex (inclusive) to toIndex (exclusive) to true and update the braille representation
    /// @param fromIndex the index of the first bit to set
    /// @param toIndex the index after the last bit to set
    @Override
    public synchronized void set(int fromIndex, int toIndex) {
        super.set(fromIndex, toIndex);
        // Update each affected character individually
        for (int i = fromIndex; i < toIndex; i++) {
            updateBrailleCharacter(i);
        }
    }

    /// Set the bits from fromIndex (inclusive) to toIndex (exclusive) to true and update the braille representation
    /// @param path the path to the file to update
    /// @param fromIndex the index of the first bit to set
    /// @param toIndex the index after the last bit to set
    public static synchronized void set(Path path, int fromIndex, int toIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.set(fromIndex, toIndex);
    }

    /// Set the bits from fromIndex (inclusive) to toIndex (exclusive) to the specified value and update the braille representation
    /// @param fromIndex the index of the first bit to set
    /// @param toIndex the index after the last bit to set
    /// @param value the value to set the bits to
    @Override
    public synchronized void set(int fromIndex, int toIndex, boolean value) {
        super.set(fromIndex, toIndex, value);
        // Update each affected character individually
        for (int i = fromIndex; i < toIndex; i++) {
            updateBrailleCharacter(i);
        }
    }

    /// Set the bits from fromIndex (inclusive) to toIndex (exclusive) to the specified value and update the braille representation
    /// @param path the path to the file to update
    /// @param fromIndex the index of the first bit to set
    /// @param toIndex the index after the last bit to set
    /// @param value the value to set the bits to
    public static synchronized void set(Path path, int fromIndex, int toIndex, boolean value) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.set(fromIndex, toIndex, value);
    }

    /// Clear the bit at the specified index and update the braille representation
    /// @param bitIndex the index of the bit to clear
    @Override
    public synchronized void clear(int bitIndex) {
        super.clear(bitIndex);
        updateBrailleCharacter(bitIndex);
    }

    /// Clear the bit at the specified index and update the braille representation
    /// @param path the path to the file to update
    /// @param bitIndex the index of the bit to clear
    public static synchronized void clear(Path path, int bitIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.clear(bitIndex);
    }

    /// Clear the bits from fromIndex (inclusive) to toIndex (exclusive) and update the braille representation
    /// @param fromIndex the index of the first bit to clear
    /// @param toIndex the index after the last bit to clear
    @Override
    public synchronized void clear(int fromIndex, int toIndex) {
        super.clear(fromIndex, toIndex);
        // Update each affected character individually
        for (int i = fromIndex; i < toIndex; i++) {
            updateBrailleCharacter(i);
        }
    }

    /// Clear the bits from fromIndex (inclusive) to toIndex (exclusive) and update the braille representation
    /// @param path the path to the file to update
    /// @param fromIndex the index of the first bit to clear
    /// @param toIndex the index after the last bit to clear
    public static synchronized void clear(Path path, int fromIndex, int toIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.clear(fromIndex, toIndex);
    }

    /// Clear all bits in the BitSet and update the braille representation
    @Override
    public synchronized void clear() {
        // Get the length before clearing
        int length = this.length();
        super.clear();

        // Update each character that might have been affected
        for (int i = 0; i < length; i++) {
            updateBrailleCharacter(i);
        }
    }

    /// Clear all bits in the BitSet and update the braille representation
    /// @param path the path to the file to update
    public static synchronized void clear(Path path) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.clear();
    }

    /// Flip the bit at the specified index and update the braille representation
    /// @param bitIndex the index of the bit to flip
    @Override
    public synchronized void flip(int bitIndex) {
        super.flip(bitIndex);
        updateBrailleCharacter(bitIndex);
    }

    /// Flip the bit at the specified index and update the braille representation
    /// @param path the path to the file to update
    /// @param bitIndex the index of the bit to flip
    public static synchronized void flip(Path path, int bitIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.flip(bitIndex);
    }

    /// Flip the bits from fromIndex (inclusive) to toIndex (exclusive) and update the braille representation
    /// @param fromIndex the index of the first bit to flip
    /// @param toIndex the index after the last bit to flip
    @Override
    public synchronized void flip(int fromIndex, int toIndex) {
        super.flip(fromIndex, toIndex);
        // Update each affected character individually
        for (int i = fromIndex; i < toIndex; i++) {
            updateBrailleCharacter(i);
        }
    }

    /// Flip the bits from fromIndex (inclusive) to toIndex (exclusive) and update the braille representation
    /// @param path the path to the file to update
    /// @param fromIndex the index of the first bit to flip
    /// @param toIndex the index after the last bit to flip
    public static synchronized void flip(Path path, int fromIndex, int toIndex) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.flip(fromIndex, toIndex);
    }

    /// Get the value of the bit at the specified index
    /// @param bitIndex the index of the bit to get
    /// @return the value of the bit at the specified index
    @Override
    public boolean get(int bitIndex) {
        return super.get(bitIndex);
    }

    /// Get a new BitSet representing the bits from fromIndex (inclusive) to toIndex (exclusive)
    /// @param fromIndex the index of the first bit to include
    /// @param toIndex the index after the last bit to include
    /// @return a new BitSet representing the specified range of bits
    @Override
    public BitSet get(int fromIndex, int toIndex) {
        return super.get(fromIndex, toIndex);
    }

    /// Returns the index of the first bit that is set to true that occurs on or after the specified index
    /// @param fromIndex the index to start checking from (inclusive)
    /// @return the index of the next set bit, or -1 if there is no such bit
    @Override
    public int nextSetBit(int fromIndex) {
        return super.nextSetBit(fromIndex);
    }

    /// Returns the index of the first bit that is set to false that occurs on or after the specified index
    /// @param fromIndex the index to start checking from (inclusive)
    /// @return the index of the next clear bit, or -1 if there is no such bit
    @Override
    public int nextClearBit(int fromIndex) {
        return super.nextClearBit(fromIndex);
    }

    /// Returns the index of the nearest bit that is set to true that occurs on or before the specified index
    /// @param fromIndex the index to start checking from (inclusive)
    /// @return the index of the previous set bit, or -1 if there is no such bit
    @Override
    public int previousSetBit(int fromIndex) {
        return super.previousSetBit(fromIndex);
    }

    /// Returns the index of the nearest bit that is set to false that occurs on or before the specified index
    /// @param fromIndex the index to start checking from (inclusive)
    /// @return the index of the previous clear bit, or -1 if there is no such bit
    @Override
    public int previousClearBit(int fromIndex) {
        return super.previousClearBit(fromIndex);
    }

    /// Returns the number of bits set to true in this BitSet
    /// @return the number of bits set to true
    @Override
    public int cardinality() {
        return super.cardinality();
    }

    /// Performs a logical AND of this BitSet with the specified BitSet and updates the braille representation
    /// @param set the BitSet to AND with
    @Override
    public synchronized void and(BitSet set) {
        // Create a copy of the current BitSet to track changes
        BitSet before = (BitSet) this.clone();

        // Perform the AND operation
        super.and(set);

        // Find the maximum length to check for changes
        int maxLength = Math.max(before.length(), this.length());

        // Update each character that might have been affected
        for (int i = 0; i < maxLength; i++) {
            if (before.get(i) != this.get(i)) {
                updateBrailleCharacter(i);
            }
        }
    }

    /// Performs a logical AND of the BitSet at the specified path with the specified BitSet and updates the braille representation
    /// @param path the path to the file to update
    /// @param set the BitSet to AND with
    public static synchronized void and(Path path, BitSet set) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.and(set);
    }

    /// Performs a logical OR of this BitSet with the specified BitSet and updates the braille representation
    /// @param set the BitSet to OR with
    @Override
    public synchronized void or(BitSet set) {
        // Create a copy of the current BitSet to track changes
        BitSet before = (BitSet) this.clone();

        // Perform the OR operation
        super.or(set);

        // Find the maximum length to check for changes
        int maxLength = Math.max(before.length(), this.length());

        // Update each character that might have been affected
        for (int i = 0; i < maxLength; i++) {
            if (before.get(i) != this.get(i)) {
                updateBrailleCharacter(i);
            }
        }
    }

    /// Performs a logical OR of the BitSet at the specified path with the specified BitSet and updates the braille representation
    /// @param path the path to the file to update
    /// @param set the BitSet to OR with
    public static synchronized void or(Path path, BitSet set) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.or(set);
    }

    /// Performs a logical XOR of this BitSet with the specified BitSet and updates the braille representation
    /// @param set the BitSet to XOR with
    @Override
    public synchronized void xor(BitSet set) {
        // Create a copy of the current BitSet to track changes
        BitSet before = (BitSet) this.clone();

        // Perform the XOR operation
        super.xor(set);

        // Find the maximum length to check for changes
        int maxLength = Math.max(before.length(), this.length());

        // Update each character that might have been affected
        for (int i = 0; i < maxLength; i++) {
            if (before.get(i) != this.get(i)) {
                updateBrailleCharacter(i);
            }
        }
    }

    /// Performs a logical XOR of the BitSet at the specified path with the specified BitSet and updates the braille representation
    /// @param path the path to the file to update
    /// @param set the BitSet to XOR with
    public static synchronized void xor(Path path, BitSet set) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.xor(set);
    }

    /// Performs a logical AND NOT of this BitSet with the specified BitSet and updates the braille representation
    /// @param set the BitSet to AND NOT with
    @Override
    public synchronized void andNot(BitSet set) {
        // Create a copy of the current BitSet to track changes
        BitSet before = (BitSet) this.clone();

        // Perform the AND NOT operation
        super.andNot(set);

        // Find the maximum length to check for changes
        int maxLength = Math.max(before.length(), this.length());

        // Update each character that might have been affected
        for (int i = 0; i < maxLength; i++) {
            if (before.get(i) != this.get(i)) {
                updateBrailleCharacter(i);
            }
        }
    }

    /// Performs a logical AND NOT of the BitSet at the specified path with the specified BitSet and updates the braille representation
    /// @param path the path to the file to update
    /// @param set the BitSet to AND NOT with
    public static synchronized void andNot(Path path, BitSet set) {
        BitSetTracker tracker = new BitSetTracker(path);
        tracker.andNot(set);
    }

    /// Returns the number of bits of space actually in use by this BitSet
    /// @return the number of bits currently in this BitSet
    @Override
    public int length() {
        return super.length();
    }

    /// Returns true if this BitSet contains no bits that are set to true
    /// @return true if this BitSet contains no bits that are set to true
    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }

    /// Returns true if the specified BitSet has any bits set to true that are also set to true in this BitSet
    /// @param set the BitSet to intersect with
    /// @return true if this BitSet intersects the specified BitSet
    @Override
    public boolean intersects(BitSet set) {
        return super.intersects(set);
    }

    /// Get the underlying BitSet
    /// @return the BitSet
    public BitSet getBitSet() {
        return this;
    }

    /// Get the file path
    /// @return the file path or null if no file is used
    public String getFilePath() {
        return useFile ? file.getPath() : null;
    }

    /// Set the file content directly
    /// @param content the content to set
    public synchronized void setFileContent(String content) {
        // Encode each character into exactly 3 bytes in UTF-8 format
        int numChars = content.length();

        // Ensure the buffer is large enough
        if (numChars * 3 > byteBuffer.capacity()) {
            expandBuffer(numChars);
        }

        // Update each character
        for (int i = 0; i < numChars; i++) {
            encodeCharTo3ByteUTF8(content.charAt(i), i);
        }

        // Remember the content length
        this.contentLength = numChars;
    }

    /// Get the braille character data
    /// @return the braille character data as a string
    public String getBrailleData() {
        // Read the data from the byte buffer
        StringBuilder sb = new StringBuilder();

        // If contentLength is set, use it to determine how many characters to return
        // Otherwise, use the BitSet to determine the length
        int numChars;
        if (contentLength > 0) {
            numChars = contentLength;
        } else {
            // Calculate the number of characters needed to represent the BitSet
            numChars = Math.max(1, (this.length() + 7) / 8); // 8 bits per braille character, rounded up, minimum 1 character

            // Limit to the buffer capacity
            numChars = Math.min(numChars, byteBuffer.capacity() / 3);
        }

        for (int i = 0; i < numChars; i++) {
            sb.append(decode3ByteUTF8(i));
        }
        return sb.toString();
    }

    /// Get the Java char equivalent of a given position
    /// @param index the index of the character to get
    /// @return the character at the specified index
    public char getCharAt(int index) {
        return decode3ByteUTF8(index);
    }
}
