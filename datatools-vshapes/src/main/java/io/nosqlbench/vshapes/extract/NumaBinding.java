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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

/**
 * Panama FFM bindings for libnuma (Linux NUMA library).
 *
 * <p>Provides thread affinity and memory placement control for NUMA-aware processing.
 * Gracefully degrades to no-ops if libnuma is not available.</p>
 *
 * <h2>Key Functions</h2>
 * <ul>
 *   <li>{@link #runOnNode(int)} - Bind current thread to a NUMA node</li>
 *   <li>{@link #setLocalAlloc()} - Set memory allocation policy to local node</li>
 *   <li>{@link #allocOnNode(long, int, Arena)} - Allocate memory on specific node</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Check if NUMA binding is available
 * if (NumaBinding.isAvailable()) {
 *     // Bind this thread to NUMA node 0
 *     NumaBinding.runOnNode(0);
 *
 *     // Allocate 1GB on node 0
 *     try (Arena arena = Arena.ofConfined()) {
 *         MemorySegment mem = NumaBinding.allocOnNode(1024L * 1024 * 1024, 0, arena);
 *         // Use memory...
 *     }
 * }
 * }</pre>
 *
 * <h2>Requirements</h2>
 * <ul>
 *   <li>Linux with libnuma installed (libnuma-dev package)</li>
 *   <li>JDK 21+ with Panama FFM enabled</li>
 * </ul>
 *
 * @see NumaTopology
 * @see NumaAwareDatasetModelExtractor
 */
public final class NumaBinding {

    private static final class LibNumaHolder {
        static final Linker LINKER;
        static final SymbolLookup LIBNUMA;
        static final boolean AVAILABLE;

        static final MethodHandle NUMA_AVAILABLE;
        static final MethodHandle NUMA_RUN_ON_NODE;
        static final MethodHandle NUMA_SET_LOCALALLOC;
        static final MethodHandle NUMA_ALLOC_ONNODE;
        static final MethodHandle NUMA_FREE;
        static final MethodHandle NUMA_MAX_NODE;

        static {
            Linker linker = null;
            SymbolLookup lookup = null;
            boolean available = false;

            MethodHandle numaAvailable = null;
            MethodHandle numaRunOnNode = null;
            MethodHandle numaSetLocalalloc = null;
            MethodHandle numaAllocOnnode = null;
            MethodHandle numaFree = null;
            MethodHandle numaMaxNode = null;

            try {
                linker = Linker.nativeLinker();
                // Try to load libnuma.so
                lookup = SymbolLookup.libraryLookup("libnuma.so.1", Arena.global());

                // int numa_available(void)
                Optional<MemorySegment> numaAvailableSym = lookup.find("numa_available");
                if (numaAvailableSym.isPresent()) {
                    numaAvailable = linker.downcallHandle(
                        numaAvailableSym.get(),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT)
                    );
                    int result = (int) numaAvailable.invoke();
                    available = (result >= 0);
                }

                if (available) {
                    numaRunOnNode = linker.downcallHandle(
                        lookup.find("numa_run_on_node").orElseThrow(),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
                    );
                    numaSetLocalalloc = linker.downcallHandle(
                        lookup.find("numa_set_localalloc").orElseThrow(),
                        FunctionDescriptor.ofVoid()
                    );
                    numaAllocOnnode = linker.downcallHandle(
                        lookup.find("numa_alloc_onnode").orElseThrow(),
                        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)
                    );
                    numaFree = linker.downcallHandle(
                        lookup.find("numa_free").orElseThrow(),
                        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                    );
                    numaMaxNode = linker.downcallHandle(
                        lookup.find("numa_max_node").orElseThrow(),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT)
                    );
                }
            } catch (Throwable e) {
                available = false;
            }

            LINKER = linker;
            LIBNUMA = lookup;
            AVAILABLE = available;
            NUMA_AVAILABLE = numaAvailable;
            NUMA_RUN_ON_NODE = numaRunOnNode;
            NUMA_SET_LOCALALLOC = numaSetLocalalloc;
            NUMA_ALLOC_ONNODE = numaAllocOnnode;
            NUMA_FREE = numaFree;
            NUMA_MAX_NODE = numaMaxNode;
        }
    }

    private static final int DETECTED_MAX_NODE;
    private static final boolean DETECTED_AVAILABLE;

    static {
        // Non-privileged topology detection via sysfs
        int maxNode = 0;
        boolean available = false;
        try {
            java.io.File nodeDir = new java.io.File("/sys/devices/system/node");
            if (nodeDir.exists() && nodeDir.isDirectory()) {
                String[] files = nodeDir.list();
                if (files != null) {
                    for (String file : files) {
                        if (file.startsWith("node") && file.matches("node\\d+")) {
                            try {
                                int id = Integer.parseInt(file.substring(4));
                                maxNode = Math.max(maxNode, id);
                                available = true;
                            } catch (NumberFormatException ignored) {}
                        }
                    }
                }
            }
        } catch (Throwable ignored) {}
        
        DETECTED_MAX_NODE = maxNode;
        DETECTED_AVAILABLE = available;
    }

    private NumaBinding() {
        // Static utility class
    }

    /**
     * Returns true if NUMA is supported and detected on this system.
     * <p>Uses non-privileged sysfs detection first.</p>
     */
    public static boolean isAvailable() {
        return DETECTED_AVAILABLE || LibNumaHolder.AVAILABLE;
    }

    /**
     * Binds the current thread to run on CPUs of the specified NUMA node.
     * <p>Triggers loading of libnuma native bindings.</p>
     */
    public static int runOnNode(int node) {
        if (!LibNumaHolder.AVAILABLE || LibNumaHolder.NUMA_RUN_ON_NODE == null) {
            return -1;
        }
        try {
            return (int) LibNumaHolder.NUMA_RUN_ON_NODE.invoke(node);
        } catch (Throwable e) {
            return -1;
        }
    }

    /**
     * Sets the memory allocation policy to prefer local node allocation.
     */
    public static void setLocalAlloc() {
        if (!LibNumaHolder.AVAILABLE || LibNumaHolder.NUMA_SET_LOCALALLOC == null) {
            return;
        }
        try {
            LibNumaHolder.NUMA_SET_LOCALALLOC.invoke();
        } catch (Throwable e) {
            // Ignore
        }
    }

    /**
     * Allocates memory on a specific NUMA node.
     */
    public static MemorySegment allocOnNode(long size, int node, Arena arena) {
        if (!LibNumaHolder.AVAILABLE || LibNumaHolder.NUMA_ALLOC_ONNODE == null) {
            return arena.allocate(size, 64);
        }

        try {
            MemorySegment ptr = (MemorySegment) LibNumaHolder.NUMA_ALLOC_ONNODE.invoke(size, node);
            if (ptr.address() == 0) {
                return null;
            }
            final MemorySegment finalPtr = ptr;
            final long finalSize = size;
            return ptr.reinterpret(size, arena, segment -> {
                try {
                    LibNumaHolder.NUMA_FREE.invoke(finalPtr, finalSize);
                } catch (Throwable e) {}
            });
        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * Returns the maximum NUMA node ID on this system.
     * <p>Uses non-privileged sysfs detection first.</p>
     */
    public static int maxNode() {
        if (DETECTED_AVAILABLE) return DETECTED_MAX_NODE;
        if (!LibNumaHolder.AVAILABLE || LibNumaHolder.NUMA_MAX_NODE == null) {
            return 0;
        }
        try {
            return (int) LibNumaHolder.NUMA_MAX_NODE.invoke();
        } catch (Throwable e) {
            return 0;
        }
    }

    /**
     * Returns the number of NUMA nodes on this system.
     */
    public static int nodeCount() {
        return maxNode() + 1;
    }

    /**
     * Convenience method to bind thread and set local allocation policy.
     */
    public static boolean bindThreadToNode(int node) {
        if (!LibNumaHolder.AVAILABLE) { // Triggers lazy load
            return false;
        }
        int result = runOnNode(node);
        if (result == 0) {
            setLocalAlloc();
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "NumaBinding{available=" + isAvailable() + ", maxNode=" + maxNode() + "}";
    }
}
