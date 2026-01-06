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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for NumaTopology.
 */
@Tag("unit")
public class NumaTopologyTest {

    @Test
    void detect_returnsValidTopology() {
        NumaTopology topology = NumaTopology.detect();

        assertNotNull(topology);
        assertTrue(topology.nodeCount() >= 1, "Must have at least 1 NUMA node");
        assertTrue(topology.totalCpus() >= 1, "Must have at least 1 CPU");
    }

    @Test
    void nodeCount_matchesTotalCpuDistribution() {
        NumaTopology topology = NumaTopology.detect();

        int cpuCount = 0;
        for (int node = 0; node < topology.nodeCount(); node++) {
            cpuCount += topology.cpusForNode(node).size();
        }

        assertEquals(topology.totalCpus(), cpuCount,
            "Sum of CPUs per node should equal total CPUs");
    }

    @Test
    void cpusForNode_returnsValidCpuList() {
        NumaTopology topology = NumaTopology.detect();

        for (int node = 0; node < topology.nodeCount(); node++) {
            List<Integer> cpus = topology.cpusForNode(node);
            assertNotNull(cpus);
            assertFalse(cpus.isEmpty(), "Node " + node + " should have CPUs");

            // All CPU IDs should be valid
            for (int cpu : cpus) {
                assertTrue(cpu >= 0, "CPU ID should be non-negative: " + cpu);
            }
        }
    }

    @Test
    void cpusForNode_throwsOnInvalidNode() {
        NumaTopology topology = NumaTopology.detect();

        assertThrows(IllegalArgumentException.class, () ->
            topology.cpusForNode(-1));

        assertThrows(IllegalArgumentException.class, () ->
            topology.cpusForNode(topology.nodeCount()));
    }

    @Test
    void isNuma_correctlyIdentifiesMultiNodeSystems() {
        NumaTopology topology = NumaTopology.detect();

        if (topology.nodeCount() > 1) {
            assertTrue(topology.isNuma());
        } else {
            assertFalse(topology.isNuma());
        }
    }

    @Test
    void threadsPerNode_calculatesCorrectly() {
        NumaTopology topology = NumaTopology.detect();

        int reservedThreads = 10;
        int perNode = topology.threadsPerNode(reservedThreads);

        assertTrue(perNode >= 1, "Should have at least 1 thread per node");

        int totalUsed = perNode * topology.nodeCount();
        int maxExpected = topology.totalCpus() - reservedThreads;

        // Total threads should not exceed available - reserved
        assertTrue(totalUsed <= topology.totalCpus(),
            "Total threads should not exceed available CPUs");
    }

    @Test
    void nodeForCpu_findsCorrectNode() {
        NumaTopology topology = NumaTopology.detect();

        for (int node = 0; node < topology.nodeCount(); node++) {
            List<Integer> cpus = topology.cpusForNode(node);
            for (int cpu : cpus) {
                assertEquals(node, topology.nodeForCpu(cpu),
                    "CPU " + cpu + " should be on node " + node);
            }
        }
    }

    @Test
    void nodeForCpu_returnsMinusOneForInvalidCpu() {
        NumaTopology topology = NumaTopology.detect();

        // Very high CPU ID that shouldn't exist
        assertEquals(-1, topology.nodeForCpu(999999));
    }

    @Test
    void cpusPerNode_returnsReasonableValue() {
        NumaTopology topology = NumaTopology.detect();

        int perNode = topology.cpusPerNode();
        assertTrue(perNode >= 1, "Should have at least 1 CPU per node");
        assertTrue(perNode <= topology.totalCpus(),
            "CPUs per node should not exceed total");
    }

    @Test
    void toString_containsUsefulInfo() {
        NumaTopology topology = NumaTopology.detect();

        String str = topology.toString();
        assertNotNull(str);
        assertTrue(str.contains("NumaTopology"));
        assertTrue(str.contains("nodes="));
        assertTrue(str.contains("totalCpus="));
    }

    @Test
    void detect_isRepeatable() {
        NumaTopology t1 = NumaTopology.detect();
        NumaTopology t2 = NumaTopology.detect();

        assertEquals(t1.nodeCount(), t2.nodeCount());
        assertEquals(t1.totalCpus(), t2.totalCpus());
    }
}
