package io.nosqlbench.command.merkle.subcommands;

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

import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Command to render a TUI-style tree view of a Merkle tree structure.
 */
@Command(
    name = "treeview",
    description = "Render a TUI-style tree view of a Merkle tree structure"
)
public class CMD_merkle_treeview implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_merkle_treeview.class);

    // File extensions for merkle tree files
    public static final String MRKL = ".mrkl";
    public static final String MREF = ".mref";

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";

    @Parameters(index = "0", description = "Merkle tree file to visualize")
    private Path file;

    @Option(names = {"--highlight"}, description = "Nodes to highlight: single node, comma-separated list, range with hyphen (closed-closed), or range with double-dot (closed..open)", required = false)
    private String highlight;

    @Option(names = {"-l", "--hash-length"}, description = "Number of bytes of hash to display (default: 16)", defaultValue = "16")
    private int hashLength;

    @Option(names = {"--base"}, description = "Base node index to start visualization from (default: 0)", defaultValue = "0")
    private int baseNode;

    @Option(names = {"--levels"}, description = "Number of levels to include in the visualization (including the base node)", defaultValue = "4")
    private int levels;

    @Override
    public Integer call() throws Exception {
        // Parse the highlight option if provided
        int startNode = -1;
        int endNode = -1;
        List<Integer> highlightNodes = new ArrayList<>();

        if (highlight != null && !highlight.isEmpty()) {
            try {
                // Check for comma-separated list
                if (highlight.contains(",")) {
                    String[] nodes = highlight.split(",");
                    for (String node : nodes) {
                        highlightNodes.add(Integer.parseInt(node.trim()));
                    }
                    if (!highlightNodes.isEmpty()) {
                        startNode = Collections.min(highlightNodes);
                        endNode = Collections.max(highlightNodes);
                    }
                }
                // Check for range with double-dot (closed..open)
                else if (highlight.contains("..")) {
                    String[] parts = highlight.split("\\.\\.");
                    if (parts.length == 2) {
                        startNode = Integer.parseInt(parts[0]);
                        // Subtract 1 from the end to make it closed..open
                        endNode = Integer.parseInt(parts[1]) - 1;
                    }
                }
                // Check for range with hyphen (closed-closed)
                else if (highlight.contains("-")) {
                    String[] parts = highlight.split("-");
                    if (parts.length == 2) {
                        startNode = Integer.parseInt(parts[0]);
                        endNode = Integer.parseInt(parts[1]);
                    }
                }
                // Single node
                else {
                    startNode = Integer.parseInt(highlight);
                    endNode = startNode;
                }
            } catch (NumberFormatException e) {
                logger.error("Invalid highlight format: {}. Expected formats: single node, comma-separated list, range with hyphen (closed-closed), or range with double-dot (closed..open)", highlight);
                return 1;
            }
        }

        // Calculate maxWidth based on levels
        int maxWidth = (int) Math.pow(2, levels - 1);

        boolean success = execute(file, startNode, endNode, hashLength, baseNode, maxWidth);
        return success ? 0 : 1;
    }

    /**
     * Execute the tree command on the specified file.
     *
     * @param file       The merkle tree file to visualize
     * @param startNode  The start of the range to highlight (inclusive), or -1 if no range
     * @param endNode    The end of the range to highlight (inclusive), or -1 if no range
     * @param hashLength The number of bytes of hash to display
     * @param baseNode   The base node index to start visualization from
     * @param maxWidth   The maximum number of nodes to display at the finest level
     * @return true if the operation was successful, false otherwise
     */
    public boolean execute(Path file, int startNode, int endNode, int hashLength, int baseNode, int maxWidth) {
        try {
            if (!Files.exists(file)) {
                logger.error("File not found: {}", file);
                return false;
            }

            // Determine the appropriate Merkle file path based on the file extension
            Path merklePath = determineMerklePath(file);

            if (!Files.exists(merklePath)) {
                logger.error("Merkle file not found for: {}", file);
                return false;
            }

            // Load the MerkleRef from the file
            MerkleDataImpl merkleRef = MerkleRefFactory.load(merklePath);

            // Get the total number of leaves
            int leafCount = merkleRef.getNumberOfLeaves();
            int totalNodes = 2 * leafCount - 1;

            // Validate the base node
            if (baseNode < 0 || baseNode >= totalNodes) {
                logger.error("Base node {} is out of bounds. Tree has {} nodes (0-{})", 
                            baseNode, totalNodes, totalNodes - 1);
                return false;
            }

            // Validate the range
            if (startNode >= 0 && endNode >= 0) {
                if (startNode >= leafCount || endNode >= leafCount) {
                    logger.error("Node range {}-{} is out of bounds. Tree has {} leaves.", 
                                startNode, endNode, leafCount);
                    return false;
                }
                if (startNode > endNode) {
                    logger.error("Invalid range: start node {} is greater than end node {}", 
                                startNode, endNode);
                    return false;
                }

                // Check if the selected base node and highlight nodes are part of the same tree branch
                int rangeSubtreeBase = findSubtreeBase(merkleRef, startNode, endNode);

                // Check if the selected base is an ancestor of the range subtree base
                boolean isAncestor = isAncestor(baseNode, rangeSubtreeBase);

                if (!isAncestor) {
                    // Find the lowest common ancestor of the base node and the range
                    int lowestCommonAncestor = findLowestCommonAncestor(baseNode, rangeSubtreeBase);

                    logger.error("Selected base node {} and highlight range {}-{} are not part of the same tree branch.", 
                                baseNode, startNode, endNode);
                    logger.error("Suggested alternatives:");
                    logger.error("  1. Use base node {} (lowest common ancestor)", lowestCommonAncestor);
                    logger.error("  2. Use base node {} (subtree containing the range)", rangeSubtreeBase);
                    return false;
                }
            }

            // Render the tree
            renderTree(merkleRef, baseNode, startNode, endNode, hashLength, maxWidth);

            return true;
        } catch (Exception e) {
            logger.error("Error rendering tree for file {}: {}", file, e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Check if node1 is an ancestor of node2 in the tree.
     *
     * @param node1 The potential ancestor node
     * @param node2 The potential descendant node
     * @return true if node1 is an ancestor of node2, false otherwise
     */
    private boolean isAncestor(int node1, int node2) {
        // If node1 is the same as node2, it's considered an ancestor
        if (node1 == node2) {
            return true;
        }

        // If node1 is greater than node2, it cannot be an ancestor
        if (node1 > node2) {
            return false;
        }

        // Traverse up from node2 to see if we reach node1
        int current = node2;
        while (current > 0) {
            current = (current - 1) / 2;
            if (current == node1) {
                return true;
            }
        }

        return false;
    }

    /**
     * Find the lowest common ancestor of two nodes in the tree.
     *
     * @param node1 The first node
     * @param node2 The second node
     * @return The index of the lowest common ancestor
     */
    private int findLowestCommonAncestor(int node1, int node2) {
        // If either node is the base, the base is the LCA
        if (node1 == 0 || node2 == 0) {
            return 0;
        }

        // If one node is an ancestor of the other, it's the LCA
        if (isAncestor(node1, node2)) {
            return node1;
        }
        if (isAncestor(node2, node1)) {
            return node2;
        }

        // Find the LCA by traversing up from both nodes
        int current1 = node1;
        int current2 = node2;

        while (current1 != current2) {
            if (current1 > current2) {
                current1 = (current1 - 1) / 2;
            } else {
                current2 = (current2 - 1) / 2;
            }
        }

        return current1;
    }

    /**
     * Find the base of the smallest subtree that contains the specified range.
     * If no range is specified, returns the base of the entire tree (index 0).
     *
     * @param merkleTree The merkle tree
     * @param startNode  The start of the range (inclusive), or -1 if no range
     * @param endNode    The end of the range (inclusive), or -1 if no range
     * @return The index of the subtree base
     */
    private int findSubtreeBase(MerkleDataImpl merkleRef, int startNode, int endNode) {
        // If no range is specified, return the base of the entire tree
        if (startNode < 0 || endNode < 0) {
            return 0;
        }

        int leafCount = merkleRef.getNumberOfLeaves();

        // Calculate the lowest common ancestor of startNode and endNode
        // First, convert leaf indices to their positions in the complete binary tree
        int startPos = leafCount - 1 + startNode;
        int endPos = leafCount - 1 + endNode;

        // Find the lowest common ancestor
        while (startPos != endPos) {
            if (startPos > endPos) {
                startPos = (startPos - 1) / 2;
            } else {
                endPos = (endPos - 1) / 2;
            }
        }

        // Convert back to the index in the merkle tree
        return startPos;
    }

    /**
     * Render a TUI-style tree view of the merkle tree structure.
     *
     * @param merkleTree The merkle tree
     * @param baseIndex  The index of the subtree base to render
     * @param startNode  The start of the range to highlight (inclusive), or -1 if no range
     * @param endNode    The end of the range to highlight (inclusive), or -1 if no range
     * @param hashLength The number of bytes of hash to display
     * @param maxWidth   The maximum number of nodes to display at the finest level
     */
    private void renderTree(MerkleDataImpl merkleRef, int baseIndex, int startNode, int endNode, int hashLength, int maxWidth) {
        int leafCount = merkleRef.getNumberOfLeaves();
        int totalNodes = 2 * leafCount - 1;

        // Create a map to store the tree structure
        Map<Integer, List<Integer>> treeMap = new HashMap<>();

        // Build the tree structure
        for (int i = 0; i < totalNodes; i++) {
            int parent = (i - 1) / 2;
            if (i > 0) {
                treeMap.computeIfAbsent(parent, k -> new ArrayList<>()).add(i);
            }
        }

        // Calculate the depth of the subtree
        int depth = calculateDepth(baseIndex, treeMap);

        // Print the tree header
        System.out.println("Merkle Tree Visualization");
        System.out.println("------------------------");
        System.out.println("Base node: " + baseIndex);
        if (startNode >= 0 && endNode >= 0) {
            if (startNode == endNode) {
                System.out.println("Highlighting node: " + startNode);
            } else {
                System.out.println("Highlighting nodes in range: " + startNode + "-" + endNode);
            }
        }
        System.out.println("Number of levels: " + levels);
        System.out.println();

        // Render the tree recursively
        renderNode(merkleRef, baseIndex, treeMap, "", "", startNode, endNode, hashLength, leafCount, maxWidth, 0);
    }

    /**
     * Calculate the depth of a subtree.
     *
     * @param baseIndex The index of the subtree base
     * @param treeMap   The tree structure map
     * @return The depth of the subtree
     */
    private int calculateDepth(int baseIndex, Map<Integer, List<Integer>> treeMap) {
        if (!treeMap.containsKey(baseIndex)) {
            return 1;
        }

        int maxChildDepth = 0;
        for (int child : treeMap.get(baseIndex)) {
            int childDepth = calculateDepth(child, treeMap);
            maxChildDepth = Math.max(maxChildDepth, childDepth);
        }

        return 1 + maxChildDepth;
    }

    /**
     * Recursively render a node and its children.
     *
     * @param merkleTree   The merkle tree
     * @param nodeIndex    The index of the current node
     * @param treeMap      The tree structure map
     * @param prefix       The prefix for the current line
     * @param childPrefix  The prefix for child lines
     * @param startNode    The start of the range to highlight (inclusive), or -1 if no range
     * @param endNode      The end of the range to highlight (inclusive), or -1 if no range
     * @param hashLength   The number of bytes of hash to display
     * @param leafCount    The total number of leaves in the tree
     * @param maxWidth     The maximum number of nodes to display at the finest level
     * @param currentDepth The current depth in the tree (0 for base)
     */
    private void renderNode(MerkleDataImpl merkleRef, int nodeIndex, Map<Integer, List<Integer>> treeMap,
                           String prefix, String childPrefix, int startNode, int endNode, 
                           int hashLength, int leafCount, int maxWidth, int currentDepth) {
        // Get the hash for this node
        byte[] hash = merkleRef.getHash(nodeIndex);

        // Determine if this is a leaf node
        boolean isLeaf = nodeIndex >= leafCount - 1;

        // Calculate the leaf index if this is a leaf node
        int leafIndex = isLeaf ? nodeIndex - (leafCount - 1) : -1;

        // Determine if this node is in the highlighted range
        boolean isHighlighted = isLeaf && startNode >= 0 && endNode >= 0 && 
                               leafIndex >= startNode && leafIndex <= endNode;

        // Format the node label
        String nodeLabel;
        if (isLeaf) {
            nodeLabel = "Leaf " + leafIndex;
        } else {
            nodeLabel = "Node " + nodeIndex;
        }

        // Format the hash (truncated to hashLength bytes)
        String hashStr = bytesToHex(hash, hashLength);

        // Apply highlighting if needed
        String displayStr;
        if (isHighlighted) {
            displayStr = ANSI_GREEN + nodeLabel + ": " + hashStr + ANSI_RESET;
        } else if (isLeaf) {
            displayStr = ANSI_BLUE + nodeLabel + ": " + hashStr + ANSI_RESET;
        } else {
            displayStr = ANSI_YELLOW + nodeLabel + ": " + hashStr + ANSI_RESET;
        }

        // Print the current node
        System.out.println(prefix + displayStr);

        // Recursively print children
        if (treeMap.containsKey(nodeIndex)) {
            // If no highlight nodes are specified and we've already shown the specified number of levels,
            // don't show any more layers
            if (startNode < 0 && endNode < 0 && currentDepth >= levels) {
                System.out.println(childPrefix + "... (deeper layers not shown)");
                return;
            }

            List<Integer> children = treeMap.get(nodeIndex);

            // If the children are leaves and there are too many of them, limit them
            boolean childrenAreLeaves = children.get(0) >= leafCount - 1;
            if (childrenAreLeaves && children.size() > maxWidth) {
                // Display a message about limiting the number of nodes
                System.out.println(childPrefix + "Showing first " + maxWidth + " of " + children.size() + " nodes at this level");

                // Show only the first maxWidth nodes
                for (int i = 0; i < maxWidth; i++) {
                    boolean isLast = (i == maxWidth - 1);
                    String newPrefix = childPrefix + (isLast ? "└── " : "├── ");
                    String newChildPrefix = childPrefix + (isLast ? "    " : "│   ");
                    renderNode(merkleRef, children.get(i), treeMap, newPrefix, newChildPrefix, 
                              startNode, endNode, hashLength, leafCount, maxWidth, currentDepth + 1);
                }

                // Show a message about the remaining nodes
                int remaining = children.size() - maxWidth;
                if (remaining > 0) {
                    System.out.println(childPrefix + "... (" + remaining + " more nodes not shown)");
                }
            } else {
                // If we're not at a leaf level or there aren't too many children, show all of them
                for (int i = 0; i < children.size(); i++) {
                    boolean isLast = (i == children.size() - 1);
                    String newPrefix = childPrefix + (isLast ? "└── " : "├── ");
                    String newChildPrefix = childPrefix + (isLast ? "    " : "│   ");
                    renderNode(merkleRef, children.get(i), treeMap, newPrefix, newChildPrefix, 
                              startNode, endNode, hashLength, leafCount, maxWidth, currentDepth + 1);
                }
            }
        }
    }

    /**
     * Determines the appropriate Merkle file path based on the file extension.
     *
     * @param file The input file path
     * @return The path to the Merkle file
     */
    private Path determineMerklePath(Path file) {
        String fileName = file.getFileName().toString();

        // If the file is already a Merkle file (.mrkl or .mref), use it directly
        if (fileName.endsWith(MRKL) || fileName.endsWith(MREF)) {
            return file;
        }

        // Otherwise, look for an associated Merkle file
        Path merklePath = file.resolveSibling(fileName + MRKL);
        if (Files.exists(merklePath)) {
            return merklePath;
        }

        // If .mrkl doesn't exist, try .mref
        Path mrefPath = file.resolveSibling(fileName + MREF);
        if (Files.exists(mrefPath)) {
            return mrefPath;
        }

        // Default to .mrkl if neither exists
        return merklePath;
    }

    /**
     * Converts a byte array to a hex string, truncated to the specified length.
     *
     * @param bytes The byte array to convert
     * @param length The maximum number of bytes to include
     * @return A hex string representation
     */
    private String bytesToHex(byte[] bytes, int length) {
        if (bytes == null) {
            return "null";
        }

        int bytesToShow = Math.min(bytes.length, length);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytesToShow; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }

        if (bytesToShow < bytes.length) {
            sb.append("...");
        }

        return sb.toString();
    }
}
