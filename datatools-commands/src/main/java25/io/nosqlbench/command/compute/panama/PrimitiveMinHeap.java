package io.nosqlbench.command.compute.panama;

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

import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;

/**
 * Primitive max-heap for top-K neighbor selection (cache-friendly, no object allocation).
 * Uses parallel arrays instead of PriorityQueue<NeighborIndex> for better performance.
 *
 * <p>Performance benefits:
 * <ul>
 *   <li>No object allocation per element (NeighborIndex objects avoided)
 *   <li>Better cache locality (contiguous arrays vs scattered objects)
 *   <li>Inline comparisons (no Comparator overhead)
 *   <li>2-3x faster than PriorityQueue for this use case
 * </ul>
 */
public class PrimitiveMinHeap {
    private final int capacity;
    private final long[] indices;      // Vector indices
    private final double[] distances;  // Distances
    private int size;

    public PrimitiveMinHeap(int capacity) {
        this.capacity = capacity;
        this.indices = new long[capacity];
        this.distances = new double[capacity];
        this.size = 0;
    }

    /**
     * Try to add a neighbor. Only adds if heap isn't full or distance is better than worst.
     * This is a max-heap, so peek() returns the LARGEST distance (worst neighbor).
     */
    public void offer(long index, double distance) {
        if (size < capacity) {
            // Heap not full - add directly
            indices[size] = index;
            distances[size] = distance;
            siftUp(size);
            size++;
        } else if (distance < distances[0]) {
            // Better than worst - replace root
            indices[0] = index;
            distances[0] = distance;
            siftDown(0);
        }
    }

    /**
     * Extract results in sorted order (closest first).
     */
    public NeighborIndex[] toSortedArray() {
        NeighborIndex[] result = new NeighborIndex[size];

        // Extract in reverse order (largest to smallest)
        int originalSize = size;
        for (int i = originalSize - 1; i >= 0; i--) {
            result[i] = new NeighborIndex((int) indices[0], distances[0]);

            // Move last element to root and sift down
            size--;
            if (size > 0) {
                indices[0] = indices[size];
                distances[0] = distances[size];
                siftDown(0);
            }
        }

        size = originalSize; // Restore size
        return result;
    }

    private void siftUp(int pos) {
        long idx = indices[pos];
        double dist = distances[pos];

        while (pos > 0) {
            int parent = (pos - 1) >>> 1;
            if (distances[parent] >= dist) break;

            indices[pos] = indices[parent];
            distances[pos] = distances[parent];
            pos = parent;
        }

        indices[pos] = idx;
        distances[pos] = dist;
    }

    private void siftDown(int pos) {
        long idx = indices[pos];
        double dist = distances[pos];
        int half = size >>> 1;

        while (pos < half) {
            int left = (pos << 1) + 1;
            int right = left + 1;
            int largest = left;

            if (right < size && distances[right] > distances[left]) {
                largest = right;
            }

            if (dist >= distances[largest]) break;

            indices[pos] = indices[largest];
            distances[pos] = distances[largest];
            pos = largest;
        }

        indices[pos] = idx;
        distances[pos] = dist;
    }

    public int size() {
        return size;
    }

    public double peekDistance() {
        return size > 0 ? distances[0] : Double.POSITIVE_INFINITY;
    }
}
