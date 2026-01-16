package io.nosqlbench.vshapes.extract;

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility for detecting NUMA (Non-Uniform Memory Access) topology on Linux systems.
 *
 * <h2>NUMA Overview</h2>
 * <p>
 * Multi-socket systems have separate memory controllers per CPU socket.
 * Memory access is faster when the CPU accesses memory on its local node
 * (local access: ~100ns) vs memory on a remote node (remote access: ~300ns).
 * </p>
 *
 * <h2>Performance Impact</h2>
 * <pre>
 * Local NUMA access:   1x latency,  120 GB/s bandwidth
 * Remote NUMA access:  3x latency,  15 GB/s bandwidth (cross-socket)
 * </pre>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * NumaTopology topo = NumaTopology.detect();
 * System.out.println("NUMA nodes: " + topo.nodeCount());
 * System.out.println("CPUs per node: " + topo.cpusPerNode());
 *
 * // Partition work across NUMA nodes
 * for (int node = 0; node < topo.nodeCount(); node++) {
 *     List<Integer> cpus = topo.cpusForNode(node);
 *     // ... bind threads to these CPUs
 * }
 * }</pre>
 *
 * @see NumaAwareDatasetModelExtractor
 */
public final class NumaTopology {

    private static final Path NUMA_NODE_DIR = Path.of("/sys/devices/system/node");
    private static final Pattern NODE_PATTERN = Pattern.compile("node(\\d+)");
    private static final Pattern CPU_RANGE_PATTERN = Pattern.compile("(\\d+)(?:-(\\d+))?");

    private final int nodeCount;
    private final List<List<Integer>> cpusPerNode;
    private final int totalCpus;

    private NumaTopology(int nodeCount, List<List<Integer>> cpusPerNode) {
        this.nodeCount = nodeCount;
        this.cpusPerNode = cpusPerNode;
        this.totalCpus = cpusPerNode.stream().mapToInt(List::size).sum();
    }

    /**
     * Detects the NUMA topology of the current system.
     * Falls back to a single-node topology if detection fails.
     *
     * @return the detected NUMA topology
     */
    public static NumaTopology detect() {
        if (!Files.isDirectory(NUMA_NODE_DIR)) {
            return singleNode();
        }

        try (Stream<Path> nodes = Files.list(NUMA_NODE_DIR)) {
            List<Integer> nodeIds = nodes
                .map(Path::getFileName)
                .map(Path::toString)
                .map(NODE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(m -> Integer.parseInt(m.group(1)))
                .sorted()
                .toList();

            if (nodeIds.isEmpty()) {
                return singleNode();
            }

            List<List<Integer>> cpusPerNode = new ArrayList<>(nodeIds.size());
            for (int nodeId : nodeIds) {
                cpusPerNode.add(readNodeCpus(nodeId));
            }

            return new NumaTopology(nodeIds.size(), cpusPerNode);

        } catch (IOException e) {
            return singleNode();
        }
    }

    /**
     * Creates a single-node topology (non-NUMA or detection failed).
     */
    private static NumaTopology singleNode() {
        int cpuCount = Runtime.getRuntime().availableProcessors();
        List<Integer> allCpus = new ArrayList<>(cpuCount);
        for (int i = 0; i < cpuCount; i++) {
            allCpus.add(i);
        }
        return new NumaTopology(1, List.of(Collections.unmodifiableList(allCpus)));
    }

    /**
     * Reads the CPU list for a specific NUMA node.
     */
    private static List<Integer> readNodeCpus(int nodeId) throws IOException {
        Path cpuListFile = NUMA_NODE_DIR.resolve("node" + nodeId).resolve("cpulist");
        if (!Files.exists(cpuListFile)) {
            return List.of();
        }

        String content = Files.readString(cpuListFile).trim();
        return parseCpuList(content);
    }

    /**
     * Parses a CPU list string like "0-15,32-47" into individual CPU IDs.
     */
    private static List<Integer> parseCpuList(String cpuList) {
        List<Integer> cpus = new ArrayList<>();

        for (String part : cpuList.split(",")) {
            Matcher m = CPU_RANGE_PATTERN.matcher(part.trim());
            if (m.matches()) {
                int start = Integer.parseInt(m.group(1));
                int end = m.group(2) != null ? Integer.parseInt(m.group(2)) : start;
                for (int cpu = start; cpu <= end; cpu++) {
                    cpus.add(cpu);
                }
            }
        }

        return Collections.unmodifiableList(cpus);
    }

    /**
     * Returns the number of NUMA nodes.
     */
    public int nodeCount() {
        return nodeCount;
    }

    /**
     * Returns true if the system has multiple NUMA nodes.
     */
    public boolean isNuma() {
        return nodeCount > 1;
    }

    /**
     * Returns the CPU IDs for a specific NUMA node.
     *
     * @param nodeId the NUMA node (0-indexed)
     * @return list of CPU IDs on that node
     */
    public List<Integer> cpusForNode(int nodeId) {
        if (nodeId < 0 || nodeId >= nodeCount) {
            throw new IllegalArgumentException("Invalid node ID: " + nodeId);
        }
        return cpusPerNode.get(nodeId);
    }

    /**
     * Returns the number of CPUs on each NUMA node (assumes balanced).
     * For asymmetric topologies, use {@link #cpusForNode(int)}.
     */
    public int cpusPerNode() {
        return cpusPerNode.get(0).size();
    }

    /**
     * Returns the total number of CPUs across all NUMA nodes.
     */
    public int totalCpus() {
        return totalCpus;
    }

    /**
     * Suggests the optimal number of threads per NUMA node.
     * Reserves some threads for system tasks.
     *
     * @param reservedThreads threads to keep free (total, not per node)
     * @return threads per NUMA node
     */
    public int threadsPerNode(int reservedThreads) {
        int usableThreads = Math.max(1, totalCpus - reservedThreads);
        return Math.max(1, usableThreads / nodeCount);
    }

    /**
     * Returns the NUMA node ID for a given CPU.
     *
     * @param cpuId the CPU ID
     * @return the NUMA node ID, or -1 if not found
     */
    public int nodeForCpu(int cpuId) {
        for (int node = 0; node < nodeCount; node++) {
            if (cpusPerNode.get(node).contains(cpuId)) {
                return node;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NumaTopology{nodes=").append(nodeCount);
        sb.append(", totalCpus=").append(totalCpus);
        for (int i = 0; i < nodeCount; i++) {
            sb.append(", node").append(i).append("=").append(cpusPerNode.get(i).size()).append(" cpus");
        }
        sb.append("}");
        return sb.toString();
    }
}
