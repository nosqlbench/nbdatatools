package io.nosqlbench.command.analyze;

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

import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

/**
 * Provider for optimized vector loading strategies.
 * Uses memory-mapped I/O with Panama FFM API on JDK 22+ for 2-4x faster loading.
 * Falls back to standard VectorFileArray loading on older JDKs.
 *
 * <p>The optimization comes from:
 * <ul>
 *   <li>Zero-copy memory mapping via Panama FFM API</li>
 *   <li>OS-level prefetching and paging</li>
 *   <li>Bulk reading instead of one-by-one</li>
 * </ul>
 */
public class VectorLoadingProvider {
    private static final Logger logger = LogManager.getLogger(VectorLoadingProvider.class);
    private static final boolean MMAP_AVAILABLE;

    // Cached MethodHandles for memory-mapped loading
    private static MethodHandle mmapLoadVectorsMethod;
    private static MethodHandle mmapLoadVectorsTransposedMethod;
    private static MethodHandle mmapLoadVectorsTransposedWithProgressMethod;
    private static Class<?> optimizedLoaderProgressCallbackClass;

    static {
        boolean available = false;
        try {
            // Check for OptimizedVectorLoader class (only present in multi-release JAR on JDK 22+)
            Class<?> loaderClass = Class.forName("io.nosqlbench.command.compute.panama.OptimizedVectorLoader");
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();

            // Cache MethodHandle for loadVectorsMemoryMapped
            MethodType loadType = MethodType.methodType(
                List.class,  // Returns List<float[]>
                Path.class,
                int.class,
                int.class
            );
            mmapLoadVectorsMethod = lookup.findStatic(loaderClass, "loadVectorsMemoryMapped", loadType);

            // Cache MethodHandle for loadVectorsTransposed (no progress)
            MethodType transposedType = MethodType.methodType(
                float[][].class,  // Returns float[][] (transposed)
                Path.class,
                int.class,
                int.class
            );
            mmapLoadVectorsTransposedMethod = lookup.findStatic(loaderClass, "loadVectorsTransposed", transposedType);

            // Cache MethodHandle for loadVectorsTransposedWithProgress
            optimizedLoaderProgressCallbackClass = Class.forName(
                "io.nosqlbench.command.compute.panama.OptimizedVectorLoader$ProgressCallback");
            MethodType transposedWithProgressType = MethodType.methodType(
                float[][].class,  // Returns float[][] (transposed)
                Path.class,
                int.class,
                int.class,
                optimizedLoaderProgressCallbackClass
            );
            mmapLoadVectorsTransposedWithProgressMethod = lookup.findStatic(
                loaderClass, "loadVectorsTransposedWithProgress", transposedWithProgressType);

            available = true;
            logger.info("Memory-mapped vector loading ENABLED - 2-4x faster I/O");
        } catch (ClassNotFoundException e) {
            logger.debug("Memory-mapped loading NOT AVAILABLE - Panama classes not found (JDK 22+ required)");
        } catch (NoSuchMethodException e) {
            logger.error("Memory-mapped loading MISCONFIGURED - method not found: {}", e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("Memory-mapped loading MISCONFIGURED - illegal access: {}", e.getMessage());
        }
        MMAP_AVAILABLE = available;
    }

    /**
     * Load vectors using the best available method.
     * Uses memory-mapped I/O when available, falls back to VectorFileArray otherwise.
     *
     * @param filePath path to the vector file
     * @param vectorArray fallback VectorFileArray (used if mmap not available)
     * @param rangeStart starting index
     * @param rangeSize number of vectors in range
     * @param sampleSize number of vectors to load (may be less than rangeSize for sampling)
     * @param random random source for sampling (only used if sampleSize &lt; rangeSize)
     * @param progressCallback callback for progress updates (may be null)
     * @return loaded vectors as float[][]
     */
    public static float[][] loadVectors(
            Path filePath,
            VectorFileArray<float[]> vectorArray,
            int rangeStart,
            int rangeSize,
            int sampleSize,
            Random random,
            ProgressCallback progressCallback) {

        // For full range loading, try memory-mapped approach first
        if (sampleSize == rangeSize && MMAP_AVAILABLE) {
            try {
                return loadVectorsMemoryMapped(filePath, rangeStart, rangeStart + rangeSize, progressCallback);
            } catch (Throwable e) {
                logger.warn("Memory-mapped loading failed, falling back to standard I/O: {}", e.getMessage());
                // Fall through to standard loading
            }
        }

        // Fall back to standard loading (or use for sampling)
        return loadVectorsStandard(vectorArray, rangeStart, rangeSize, sampleSize, random, progressCallback);
    }

    /**
     * Load vectors using memory-mapped I/O.
     */
    @SuppressWarnings("unchecked")
    private static float[][] loadVectorsMemoryMapped(
            Path filePath,
            int startIndex,
            int endIndex,
            ProgressCallback progressCallback) throws Throwable {

        if (progressCallback != null) {
            progressCallback.onProgress(0.0, "Memory-mapping vector file...");
        }

        long startTime = System.currentTimeMillis();

        // Use cached MethodHandle for zero-overhead dispatch
        List<float[]> vectorList = (List<float[]>) mmapLoadVectorsMethod.invoke(filePath, startIndex, endIndex);

        long elapsed = System.currentTimeMillis() - startTime;
        int count = vectorList.size();

        if (progressCallback != null) {
            progressCallback.onProgress(1.0, String.format("Loaded %d vectors via mmap in %d ms", count, elapsed));
        }

        // Convert List to array
        return vectorList.toArray(new float[0][]);
    }

    /**
     * Load vectors using standard VectorFileArray (with optional sampling).
     */
    private static float[][] loadVectorsStandard(
            VectorFileArray<float[]> vectorArray,
            int rangeStart,
            int rangeSize,
            int sampleSize,
            Random random,
            ProgressCallback progressCallback) {

        float[][] data;

        if (sampleSize == rangeSize) {
            // Load all vectors in range
            data = new float[rangeSize][];
            for (int i = 0; i < rangeSize; i++) {
                data[i] = vectorArray.get(rangeStart + i);

                // Progress updates every 10%
                if (progressCallback != null && (i + 1) % Math.max(1, rangeSize / 10) == 0) {
                    double progress = (i + 1.0) / rangeSize;
                    progressCallback.onProgress(progress, String.format("Loaded %d / %d vectors", i + 1, rangeSize));
                }
            }
        } else {
            // Random sampling with reservoir sampling
            int[] sampleIndices = new int[sampleSize];

            for (int i = 0; i < sampleSize; i++) {
                sampleIndices[i] = i;
            }
            for (int i = sampleSize; i < rangeSize; i++) {
                int j = random.nextInt(i + 1);
                if (j < sampleSize) {
                    sampleIndices[j] = i;
                }
            }

            data = new float[sampleSize][];
            for (int i = 0; i < sampleSize; i++) {
                data[i] = vectorArray.get(rangeStart + sampleIndices[i]);

                if (progressCallback != null && (i + 1) % Math.max(1, sampleSize / 10) == 0) {
                    double progress = (i + 1.0) / sampleSize;
                    progressCallback.onProgress(progress, String.format("Sampled %d / %d vectors", i + 1, sampleSize));
                }
            }
        }

        return data;
    }

    /**
     * Check if memory-mapped loading is available.
     */
    public static boolean isMemoryMappedAvailable() {
        return MMAP_AVAILABLE;
    }

    /**
     * Load vectors in transposed (column-major) format.
     *
     * <p>Returns data as {@code float[dimension][vectorCount]} instead of
     * {@code float[vectorCount][dimension]}. This format is optimal for
     * per-dimension analysis operations like statistical fitting.
     *
     * <p>Uses memory-mapped I/O when available (2-4x faster), falling back to
     * standard loading with in-memory transpose otherwise.
     *
     * @param filePath path to the vector file
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @return transposed vectors, shape {@code [dimension][count]}
     */
    public static float[][] loadVectorsTransposed(Path filePath, int startIndex, int endIndex) {
        return loadVectorsTransposed(filePath, startIndex, endIndex, null);
    }

    /**
     * Load vectors in transposed (column-major) format with progress reporting.
     *
     * <p>Returns data as {@code float[dimension][vectorCount]} instead of
     * {@code float[vectorCount][dimension]}. This format is optimal for
     * per-dimension analysis operations like statistical fitting.
     *
     * <p>Uses memory-mapped I/O when available (2-4x faster), falling back to
     * standard loading with in-memory transpose otherwise.
     *
     * @param filePath path to the vector file
     * @param startIndex starting vector index (inclusive)
     * @param endIndex ending vector index (exclusive)
     * @param progressCallback callback for progress updates (may be null)
     * @return transposed vectors, shape {@code [dimension][count]}
     */
    public static float[][] loadVectorsTransposed(
            Path filePath,
            int startIndex,
            int endIndex,
            ProgressCallback progressCallback) {

        if (MMAP_AVAILABLE) {
            try {
                return loadVectorsTransposedMemoryMapped(filePath, startIndex, endIndex, progressCallback);
            } catch (Throwable e) {
                logger.warn("Memory-mapped transposed loading failed, falling back to in-memory transpose: {}",
                    e.getMessage());
                // Fall through to fallback
            }
        }

        // Fallback: load row-major and transpose in memory
        return loadVectorsTransposedFallback(filePath, startIndex, endIndex, progressCallback);
    }

    /**
     * Load transposed vectors using memory-mapped I/O.
     */
    private static float[][] loadVectorsTransposedMemoryMapped(
            Path filePath,
            int startIndex,
            int endIndex,
            ProgressCallback progressCallback) throws Throwable {

        if (progressCallback != null) {
            progressCallback.onProgress(0.0, "Memory-mapping vector file (transposed)...");
        }

        long startTime = System.currentTimeMillis();

        float[][] transposed;

        // Try to use progress-aware method if available
        if (mmapLoadVectorsTransposedWithProgressMethod != null && progressCallback != null) {
            // Create a proxy callback that adapts our ProgressCallback to the loader's callback
            Object loaderCallback = createProgressCallbackProxy(progressCallback);
            transposed = (float[][]) mmapLoadVectorsTransposedWithProgressMethod.invoke(
                filePath, startIndex, endIndex, loaderCallback);
        } else {
            // Fallback to non-progress method
            transposed = (float[][]) mmapLoadVectorsTransposedMethod.invoke(filePath, startIndex, endIndex);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        int vectorCount = transposed.length > 0 ? transposed[0].length : 0;

        if (progressCallback != null) {
            progressCallback.onProgress(1.0, String.format(
                "Loaded %d vectors (transposed) via mmap in %d ms", vectorCount, elapsed));
        }

        return transposed;
    }

    /**
     * Creates a proxy that implements OptimizedVectorLoader.ProgressCallback
     * and delegates to our ProgressCallback.
     */
    private static Object createProgressCallbackProxy(ProgressCallback ourCallback) {
        return java.lang.reflect.Proxy.newProxyInstance(
            VectorLoadingProvider.class.getClassLoader(),
            new Class<?>[] { optimizedLoaderProgressCallbackClass },
            (proxy, method, args) -> {
                if ("onProgress".equals(method.getName()) && args.length == 3) {
                    double progress = (Double) args[0];
                    int loaded = (Integer) args[1];
                    int total = (Integer) args[2];
                    ourCallback.onProgress(progress, String.format("Loaded %,d / %,d vectors", loaded, total));
                }
                return null;
            }
        );
    }

    /**
     * Fallback: load row-major vectors and transpose in memory.
     * Used when memory-mapped transposed loading is not available.
     */
    @SuppressWarnings("unchecked")
    private static float[][] loadVectorsTransposedFallback(
            Path filePath,
            int startIndex,
            int endIndex,
            ProgressCallback progressCallback) {

        if (progressCallback != null) {
            progressCallback.onProgress(0.0, "Loading vectors (fallback mode)...");
        }

        float[][] rows;
        try {
            // Try to use mmap for row-major loading if available
            if (MMAP_AVAILABLE) {
                List<float[]> vectorList = (List<float[]>) mmapLoadVectorsMethod.invoke(filePath, startIndex, endIndex);
                rows = vectorList.toArray(new float[0][]);
            } else {
                // True fallback: use VectorFileIO for standard file loading
                logger.debug("Using VectorFileIO fallback for transposed loading");
                try (VectorFileArray<float[]> reader = io.nosqlbench.nbdatatools.api.services.VectorFileIO.randomAccess(
                        io.nosqlbench.nbdatatools.api.services.FileType.xvec, float[].class, filePath)) {
                    int count = endIndex - startIndex;
                    rows = new float[count][];
                    for (int i = 0; i < count; i++) {
                        rows[i] = reader.get(startIndex + i);
                        if (progressCallback != null && i > 0 && i % Math.max(1, count / 10) == 0) {
                            progressCallback.onProgress(0.25 * i / count,
                                String.format("Loaded %,d / %,d vectors", i, count));
                        }
                    }
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to load vectors: " + e.getMessage(), e);
        }

        if (rows.length == 0) {
            return new float[0][0];
        }

        if (progressCallback != null) {
            progressCallback.onProgress(0.5, "Transposing vectors in memory...");
        }

        // Transpose row-major to column-major
        int vectorCount = rows.length;
        int dimensions = rows[0].length;
        float[][] transposed = new float[dimensions][vectorCount];

        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                transposed[d][v] = rows[v][d];
            }
        }

        if (progressCallback != null) {
            progressCallback.onProgress(1.0, String.format(
                "Loaded and transposed %d vectors", vectorCount));
        }

        return transposed;
    }

    /**
     * Callback for progress updates during loading.
     */
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(double progress, String message);
    }
}
