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

    private static final Linker LINKER;
    private static final SymbolLookup LIBNUMA;
    private static final boolean AVAILABLE;

    // Function handles (null if libnuma not available)
    private static final MethodHandle NUMA_AVAILABLE;
    private static final MethodHandle NUMA_RUN_ON_NODE;
    private static final MethodHandle NUMA_SET_LOCALALLOC;
    private static final MethodHandle NUMA_ALLOC_ONNODE;
    private static final MethodHandle NUMA_FREE;
    private static final MethodHandle NUMA_MAX_NODE;

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

                // Check if NUMA is actually available on this system
                int result = (int) numaAvailable.invoke();
                available = (result >= 0);
            }

            if (available) {
                // int numa_run_on_node(int node)
                numaRunOnNode = linker.downcallHandle(
                    lookup.find("numa_run_on_node").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
                );

                // void numa_set_localalloc(void)
                numaSetLocalalloc = linker.downcallHandle(
                    lookup.find("numa_set_localalloc").orElseThrow(),
                    FunctionDescriptor.ofVoid()
                );

                // void *numa_alloc_onnode(size_t size, int node)
                numaAllocOnnode = linker.downcallHandle(
                    lookup.find("numa_alloc_onnode").orElseThrow(),
                    FunctionDescriptor.of(
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.JAVA_INT
                    )
                );

                // void numa_free(void *mem, size_t size)
                numaFree = linker.downcallHandle(
                    lookup.find("numa_free").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );

                // int numa_max_node(void)
                numaMaxNode = linker.downcallHandle(
                    lookup.find("numa_max_node").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT)
                );
            }

        } catch (Throwable e) {
            // libnuma not available - that's fine, we'll use fallback behavior
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

    private NumaBinding() {
        // Static utility class
    }

    /**
     * Returns true if libnuma is available and NUMA is supported on this system.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Binds the current thread to run on CPUs of the specified NUMA node.
     *
     * <p>After this call, the thread will only be scheduled on CPUs belonging
     * to the specified node. This ensures memory accesses are local.</p>
     *
     * @param node the NUMA node (0-indexed)
     * @return 0 on success, -1 on failure (or if libnuma unavailable)
     */
    public static int runOnNode(int node) {
        if (!AVAILABLE || NUMA_RUN_ON_NODE == null) {
            return -1;
        }
        try {
            return (int) NUMA_RUN_ON_NODE.invoke(node);
        } catch (Throwable e) {
            return -1;
        }
    }

    /**
     * Sets the memory allocation policy to prefer local node allocation.
     *
     * <p>After this call, memory allocations will be placed on the NUMA node
     * where the allocating thread is running (first-touch policy).</p>
     */
    public static void setLocalAlloc() {
        if (!AVAILABLE || NUMA_SET_LOCALALLOC == null) {
            return;
        }
        try {
            NUMA_SET_LOCALALLOC.invoke();
        } catch (Throwable e) {
            // Ignore
        }
    }

    /**
     * Allocates memory on a specific NUMA node.
     *
     * <p>The returned memory segment is backed by memory physically located
     * on the specified NUMA node, ensuring local access for threads on that node.</p>
     *
     * @param size  number of bytes to allocate
     * @param node  NUMA node for placement
     * @param arena arena for lifecycle management (should be confined to one thread)
     * @return memory segment on the specified node, or null if allocation failed
     */
    public static MemorySegment allocOnNode(long size, int node, Arena arena) {
        if (!AVAILABLE || NUMA_ALLOC_ONNODE == null) {
            // Fall back to regular allocation
            return arena.allocate(size, 64);
        }

        try {
            MemorySegment ptr = (MemorySegment) NUMA_ALLOC_ONNODE.invoke(size, node);
            if (ptr.address() == 0) {
                return null;
            }

            // Reinterpret with the arena's scope and a cleanup action
            final MemorySegment finalPtr = ptr;
            final long finalSize = size;
            return ptr.reinterpret(size, arena, segment -> {
                try {
                    NUMA_FREE.invoke(finalPtr, finalSize);
                } catch (Throwable e) {
                    // Ignore cleanup errors
                }
            });

        } catch (Throwable e) {
            return null;
        }
    }

    /**
     * Returns the maximum NUMA node ID on this system.
     *
     * @return max node ID (0-indexed), or 0 if NUMA unavailable
     */
    public static int maxNode() {
        if (!AVAILABLE || NUMA_MAX_NODE == null) {
            return 0;
        }
        try {
            return (int) NUMA_MAX_NODE.invoke();
        } catch (Throwable e) {
            return 0;
        }
    }

    /**
     * Returns the number of NUMA nodes on this system.
     *
     * @return node count (at least 1)
     */
    public static int nodeCount() {
        return maxNode() + 1;
    }

    /**
     * Convenience method to bind thread and set local allocation policy.
     *
     * @param node the NUMA node to bind to
     * @return true if binding succeeded
     */
    public static boolean bindThreadToNode(int node) {
        if (!AVAILABLE) {
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
        return "NumaBinding{available=" + AVAILABLE + ", maxNode=" + maxNode() + "}";
    }
}
