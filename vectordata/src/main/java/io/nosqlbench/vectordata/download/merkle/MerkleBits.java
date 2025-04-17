package io.nosqlbench.vectordata.download.merkle;

import java.util.BitSet;

/**
 * A read-only view of a BitSet for tracking the state of merkle tree chunks.
 * This class extends BitSet but overrides all mutation methods to throw UnsupportedOperationException.
 */
public class MerkleBits extends BitSet {

    /**
     * Creates a new MerkleBits with the specified initial size.
     *
     * @param nbits The initial size of the bit set
     */
    public MerkleBits(int nbits) {
        super(nbits);
    }

    /**
     * Creates a new MerkleBits with a copy of the given BitSet.
     *
     * @param bits The BitSet to copy
     */
    public MerkleBits(BitSet bits) {
        super(0); // Create with minimal size, then copy the bits
        if (bits != null) {
            super.or(bits); // Copy all bits from the source BitSet
        }
    }

    /**
     * Creates a new MerkleBits with a copy of the given BitSet.
     * Changes to the original BitSet will not be reflected in this view.
     *
     * @param bits The BitSet to copy
     * @return A new MerkleBits with a copy of the given BitSet
     */
    public static MerkleBits copyOf(BitSet bits) {
        return new MerkleBits(bits);
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void set(int bitIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void set(int bitIndex, boolean value) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void set(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void set(int fromIndex, int toIndex, boolean value) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void clear(int bitIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void clear(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void flip(int bitIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void flip(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void and(BitSet set) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void or(BitSet set) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void xor(BitSet set) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Throws UnsupportedOperationException because MerkleBits is read-only.
     *
     * @throws UnsupportedOperationException Always thrown because MerkleBits is read-only
     */
    @Override
    public void andNot(BitSet set) {
        throw new UnsupportedOperationException("MerkleBits is read-only");
    }

    /**
     * Returns a new MerkleBits that is the result of performing a logical AND of this MerkleBits with the given BitSet.
     *
     * @param set The BitSet to AND with
     * @return A new MerkleBits representing the intersection of this MerkleBits and the given BitSet
     */
    public MerkleBits andResult(BitSet set) {
        // Create a new BitSet from this one
        BitSet result = (BitSet) clone();
        // Create a temporary BitSet to perform the operation
        BitSet temp = new BitSet();
        temp.or(result); // Copy all bits from result to temp
        temp.and(set);   // Perform the AND operation
        // Create a new MerkleBits from the result
        return new MerkleBits(temp);
    }

    /**
     * Returns a new MerkleBits that is the result of performing a logical OR of this MerkleBits with the given BitSet.
     *
     * @param set The BitSet to OR with
     * @return A new MerkleBits representing the union of this MerkleBits and the given BitSet
     */
    public MerkleBits orResult(BitSet set) {
        // Create a new BitSet from this one
        BitSet result = (BitSet) clone();
        // Create a temporary BitSet to perform the operation
        BitSet temp = new BitSet();
        temp.or(result); // Copy all bits from result to temp
        temp.or(set);    // Perform the OR operation
        // Create a new MerkleBits from the result
        return new MerkleBits(temp);
    }

    /**
     * Returns a new MerkleBits that is the result of performing a logical XOR of this MerkleBits with the given BitSet.
     *
     * @param set The BitSet to XOR with
     * @return A new MerkleBits representing the symmetric difference of this MerkleBits and the given BitSet
     */
    public MerkleBits xorResult(BitSet set) {
        // Create a new BitSet from this one
        BitSet result = (BitSet) clone();
        // Create a temporary BitSet to perform the operation
        BitSet temp = new BitSet();
        temp.or(result); // Copy all bits from result to temp
        temp.xor(set);   // Perform the XOR operation
        // Create a new MerkleBits from the result
        return new MerkleBits(temp);
    }

    /**
     * Returns a new MerkleBits that is the result of performing a logical AND NOT of this MerkleBits with the given BitSet.
     *
     * @param set The BitSet to AND NOT with
     * @return A new MerkleBits representing the asymmetric difference of this MerkleBits and the given BitSet
     */
    public MerkleBits andNotResult(BitSet set) {
        // Create a new BitSet from this one
        BitSet result = (BitSet) clone();
        // Create a temporary BitSet to perform the operation
        BitSet temp = new BitSet();
        temp.or(result);  // Copy all bits from result to temp
        temp.andNot(set); // Perform the AND NOT operation
        // Create a new MerkleBits from the result
        return new MerkleBits(temp);
    }

    /**
     * Returns a new MerkleBits that is a copy of this MerkleBits with all bits flipped.
     *
     * @return A new MerkleBits with all bits flipped
     */
    public MerkleBits notResult() {
        // Create a new BitSet from this one
        BitSet result = (BitSet) clone();
        // Create a temporary BitSet to perform the operation
        BitSet temp = new BitSet();
        temp.or(result);        // Copy all bits from result to temp
        temp.flip(0, temp.size() > 0 ? temp.size() : 1); // Perform the flip operation
        // Create a new MerkleBits from the result
        return new MerkleBits(temp);
    }
}
