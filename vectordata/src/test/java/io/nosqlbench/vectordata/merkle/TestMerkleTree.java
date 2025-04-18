package io.nosqlbench.vectordata.merkle;

/**
 * A simple implementation of MerkleTree for testing purposes.
 */
public class TestMerkleTree extends MerkleTree {
    private final long chunkSize;
    private final long totalSize;
    private final MerkleNode root;

    public TestMerkleTree(long chunkSize, long totalSize) {
        super(createRootNode(), chunkSize, totalSize, new MerkleRange(0, totalSize));
        this.chunkSize = chunkSize;
        this.totalSize = totalSize;
        this.root = super.root();
    }

    private static MerkleNode createRootNode() {
        // Create a simple root node with empty hash
        byte[] emptyHash = new byte[MerkleNode.HASH_SIZE];
        return new MerkleNode(0, 6, emptyHash, null, null);
    }

    @Override
    public int getChunkIndexForPosition(long position) {
        return (int) (position / chunkSize);
    }

    @Override
    public NodeBoundary getBoundariesForLeaf(int leafIndex) {
        long start = leafIndex * chunkSize;
        long end = Math.min((leafIndex + 1) * chunkSize, totalSize);
        return new NodeBoundary(start, end);
    }
}
