package io.nosqlbench.command.analyze.subcommands;

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


import io.nosqlbench.command.analyze.subcommands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.command.analyze.subcommands.verify_knn.datatypes.NeighborIndex;
import io.nosqlbench.command.analyze.subcommands.verify_knn.options.ErrorMode;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusMode;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusView;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewLanterna;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewNoOp;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewRouter;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewStdout;
import io.nosqlbench.command.common.CommandLineFormatter;
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.vectordata.discovery.DatasetLoader;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.layout.FInterval;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.IntVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/// This command efficiently verifies KNN answer keys across multiple profiles in a dataset.
/// It loads the base vectors once and tests all profiles against them in a single pass,
/// activating and deactivating profiles based on their base vector ranges.
@CommandLine.Command(name = "verify_profiles",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "efficiently verify KNN answer-keys across multiple profiles",
    description = "Reads a dataset through the vectordata API (supports dataset.yaml with xvec files,\n" +
        "and remote URLs), discovers all profiles (or uses specified profiles), and efficiently\n" +
        "verifies the KNN neighborhoods for each profile.\n\n" +
        "Unlike verify_knn which processes one profile at a time, this command processes all\n" +
        "profiles in a single pass over the base vectors, significantly improving performance\n" +
        "when multiple profiles need to be verified.\n\n" +
        "The command tracks which profiles are active at each position in the base vectors,\n" +
        "based on their configured ranges. Smaller profiles are verified only while their\n" +
        "range is active, while larger profiles continue verification further into the\n" +
        "base vector iteration.\n\n" +
        "This is particularly useful for datasets with multiple size profiles (e.g., 1M, 10M,\n" +
        "100M vectors) that share the same base vector file but use different ranges.\n\n" +
        "Supports loading datasets from:\n" +
        "- Local directories with dataset.yaml (xvec format)\n" +
        "- Remote URLs (with automatic caching)",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: all tested neighborhoods across all profiles were correct",
        "2: at least one tested neighborhood was incorrect in any profile"
    })
public class CMD_analyze_verifyprofiles implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_analyze_verifyprofiles.class);

    @Parameters(description = "The dataset file(s), directory, or URL to load (supports local paths and remote URLs)", arity = "1..*")
    private List<String> datasetPaths = new ArrayList<>();

    @Option(names = {"--profiles"},
        description = "Comma-separated list of profile names to verify (default: all profiles)",
        split = ",")
    private List<String> profileNames = new ArrayList<>();

    @CommandLine.Mixin
    private RangeOption queryRangeOption = new RangeOption();

    @Option(names = {"-d", "--distance_function"},
        description = "Valid values: ${COMPLETION-CANDIDATES}")
    private DistanceFunction distanceFunction;

    @Option(names = {"-max_k", "--neighborhood_size"},
        defaultValue = "-1",
        description = "The neighborhood size (auto-detected from indices file if not specified)")
    private int K;

    @Option(names = {"-l", "--buffer_limit"},
        defaultValue = "-1",
        description = "The buffer size to retain between sorts by distance, selected automatically "
            + "when unset as a power of ten such that 10 chunks are needed for processing "
            + "each query")
    private int buffer_limit;

    @Option(names = {"-s", "--status"},
        defaultValue = "Stdout",
        description = "Valid values: ${COMPLETION-CANDIDATES}")
    private StatusMode output;

    @Option(names = {"-e", "--error_mode"},
        defaultValue = "fail",
        description = "Valid values: ${COMPLETION-CANDIDATES}")
    private ErrorMode errorMode;

    @Option(names = {"-p", "--phi"}, defaultValue = "0.001d",
        description = "When comparing values which are not exact, due to floating point rounding\n" +
            "errors, the distance within which the values are considered effectively\n" +
            "the same.")
    private double phi;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        // Print the command line being executed
        CommandLineFormatter.printCommandLine(spec);
        logger.info("");

        int totalErrors = 0;

        for (String datasetPath : datasetPaths) {
            try (ProfileSelector dataGroup = DatasetLoader.load(datasetPath)) {
                int errors = verifyProfiles(dataGroup);
                totalErrors += errors;

                if (totalErrors > 0 && errorMode == ErrorMode.Fail) {
                    break;
                }
            } catch (Exception e) {
                logger.error("Failed to process dataset {}: {}", datasetPath, e.getMessage());
                throw new RuntimeException("Failed to process dataset: " + datasetPath, e);
            }
        }

        return totalErrors > 0 ? 2 : 0;
    }


    /**
     * Verify all profiles in the given dataset
     */
    private int verifyProfiles(ProfileSelector dataGroup) throws Exception {
        // Discover profiles to verify
        Set<String> profilesToVerify = determineProfilesToVerify(dataGroup);

        if (profilesToVerify.isEmpty()) {
            logger.warn("No profiles found to verify");
            return 0;
        }

        logger.info("Verifying {} profile(s): {}", profilesToVerify.size(),
            String.join(", ", profilesToVerify));

        // Load profile states
        List<ProfileState> profileStates = new ArrayList<>();
        for (String profileName : profilesToVerify) {
            try {
                TestDataView profile = dataGroup.profile(profileName);
                ProfileState state = loadProfileState(profileName, profile);
                profileStates.add(state);
                logger.info("Loaded profile '{}': {} queries, base range [{}, {}), K={}",
                    profileName, state.queryVectors.getCount(),
                    state.baseRangeStart, state.baseRangeEnd, state.effectiveK);
            } catch (Exception e) {
                logger.error("Failed to load profile '{}': {}", profileName, e.getMessage());
                if (errorMode == ErrorMode.Fail) {
                    throw e;
                }
            }
        }

        if (profileStates.isEmpty()) {
            logger.error("No profiles could be loaded");
            return 1;
        }

        // Sort profiles by their base range start for efficient processing
        profileStates.sort(Comparator.comparingLong(p -> p.baseRangeStart));

        // Determine the overall base vector range needed
        long minBaseIndex = profileStates.stream()
            .mapToLong(p -> p.baseRangeStart)
            .min()
            .orElse(0);
        long maxBaseIndex = profileStates.stream()
            .mapToLong(p -> p.baseRangeEnd)
            .max()
            .orElse(0);

        logger.info("Overall base vector range: [{}, {})", minBaseIndex, maxBaseIndex);

        // Get the base vectors from the first profile (they all share the same base)
        FloatVectors baseVectors = profileStates.get(0).baseVectors;

        // Efficient single-pass verification
        int errors = efficientVerification(profileStates, baseVectors, minBaseIndex, maxBaseIndex);

        return errors;
    }

    /**
     * Get profile names from a ProfileSelector
     */
    private Set<String> getProfileNames(ProfileSelector dataGroup) {
        Set<String> names = dataGroup.profileNames();
        if (names.isEmpty()) {
            return Set.of("default");
        }
        return names;
    }

    /**
     * Determine which profiles to verify based on user input and available profiles
     */
    private Set<String> determineProfilesToVerify(ProfileSelector dataGroup) {
        Set<String> availableProfiles = getProfileNames(dataGroup);

        if (profileNames.isEmpty()) {
            // Verify all profiles
            return new LinkedHashSet<>(availableProfiles);
        } else {
            // Verify only specified profiles
            Set<String> result = new LinkedHashSet<>();
            for (String profileName : profileNames) {
                if (availableProfiles.contains(profileName)) {
                    result.add(profileName);
                } else {
                    logger.warn("Profile '{}' not found in dataset. Available profiles: {}",
                        profileName, String.join(", ", availableProfiles));
                }
            }
            return result;
        }
    }

    /**
     * Load the state for a single profile
     */
    private ProfileState loadProfileState(String profileName, TestDataView profile) {
        ProfileState state = new ProfileState();
        state.profileName = profileName;
        state.profile = profile;

        // Load vectors
        state.baseVectors = profile.getBaseVectors()
            .map(bv -> (FloatVectors) bv)
            .orElseThrow(() -> new RuntimeException("Base vectors not found for profile: " + profileName));

        state.queryVectors = profile.getQueryVectors()
            .map(qv -> (FloatVectors) qv)
            .orElseThrow(() -> new RuntimeException("Query vectors not found for profile: " + profileName));

        state.neighborIndices = profile.getNeighborIndices()
            .map(ni -> (IntVectors) ni)
            .orElseThrow(() -> new RuntimeException("Neighbor indices not found for profile: " + profileName));

        // Determine distance function
        state.distanceFunction = (distanceFunction != null)
            ? distanceFunction
            : profile.getDistanceFunction();

        // Determine base vector range from the base vectors' actual count (respects window)
        state.baseRangeStart = 0;
        state.baseRangeEnd = state.baseVectors.getCount();

        // Auto-detect and validate neighborhood size
        int detectedK = state.neighborIndices.get(0).length;
        state.effectiveK = validateAndSetNeighborhoodSize(detectedK);

        // Determine query range - default to just the first query for quick verification
        RangeOption.Range effectiveQueryRange = queryRangeOption.isRangeSpecified()
            ? queryRangeOption.getRange()
            : new RangeOption.Range(0, 1);  // Default: verify only first query
        state.queryRangeStart = effectiveQueryRange.start();
        state.queryRangeEnd = effectiveQueryRange.end();

        // Initialize KNN buffers for each query
        state.knnBuffers = new ArrayList<>();
        for (long i = state.queryRangeStart; i < state.queryRangeEnd; i++) {
            state.knnBuffers.add(new ArrayList<>());
        }

        // Batch-read all query vectors upfront for performance
        state.queryVectorArray = (float[][]) state.queryVectors.getRange(
            state.queryRangeStart, state.queryRangeEnd);

        return state;
    }

    /**
     * Efficiently verify all profiles using a chunked approach to manage memory
     */
    private int efficientVerification(List<ProfileState> profileStates,
                                       FloatVectors baseVectors,
                                       long minBaseIndex,
                                       long maxBaseIndex) throws Exception {

        int totalErrors = 0;

        logger.info("Starting efficient multi-profile verification");
        logger.info("Processing {} base vectors across {} profile(s)",
            maxBaseIndex - minBaseIndex, profileStates.size());
        logger.info("Profiles will be activated as their ranges become active");

        // Determine chunk size and buffer limit for base vectors
        long totalBaseVectors = maxBaseIndex - minBaseIndex;
        int chunkSize = computeBufferLimit((int) Math.min(totalBaseVectors, Integer.MAX_VALUE));
        // Initialize buffer_limit if not specified (MUST be done before any trimming)
        int originalBufferLimit = buffer_limit;
        if (buffer_limit <= 0) {
            buffer_limit = chunkSize;
        }
        logger.info("Using chunk size: {}, buffer_limit: {} {} (processing {} chunks)",
            chunkSize, buffer_limit,
            originalBufferLimit <= 0 ? "(auto)" : "(user-specified)",
            (totalBaseVectors + chunkSize - 1) / chunkSize);

        Set<ProfileState> activeProfiles = new LinkedHashSet<>();
        Set<ProfileState> completedProfiles = new LinkedHashSet<>();

        // Process base vectors in chunks with async prefetching
        int chunkNumber = 0;
        java.util.concurrent.Future<float[][]> nextChunkFuture = null;

        for (long chunkStart = minBaseIndex; chunkStart < maxBaseIndex; chunkStart += chunkSize) {
            long chunkEnd = Math.min(chunkStart + chunkSize, maxBaseIndex);
            chunkNumber++;
            logger.info("Processing chunk {}/{} [{}, {}) with {} active profile(s)",
                chunkNumber, (totalBaseVectors + chunkSize - 1) / chunkSize,
                chunkStart, chunkEnd, activeProfiles.size());

            // Update active/completed profiles for this chunk
            for (ProfileState profile : profileStates) {
                // Check if profile becomes active in this chunk
                if (chunkStart <= profile.baseRangeEnd && chunkEnd > profile.baseRangeStart) {
                    if (!activeProfiles.contains(profile) && !completedProfiles.contains(profile)) {
                        activeProfiles.add(profile);
                        logger.info("  → Activated profile '{}'",
                            profile.profileName);
                    }
                }
            }

            // Get current chunk (either from prefetch or read now)
            float[][] chunkVectors;
            if (nextChunkFuture != null) {
                // Use prefetched data
                chunkVectors = nextChunkFuture.get();
            } else {
                // First chunk - read synchronously
                chunkVectors = (float[][]) baseVectors.getRange(chunkStart, chunkEnd);
            }

            // Start prefetching next chunk while we process this one
            long nextChunkStart = chunkStart + chunkSize;
            if (nextChunkStart < maxBaseIndex) {
                long nextChunkEnd = Math.min(nextChunkStart + chunkSize, maxBaseIndex);
                final long start = nextChunkStart;
                final long end = nextChunkEnd;
                nextChunkFuture = baseVectors.getRangeAsync(start, end);
            } else {
                nextChunkFuture = null;
            }

            // Process each base vector in the chunk for all active profiles
            long lastProgressReport = System.currentTimeMillis();
            for (int i = 0; i < chunkVectors.length; i++) {
                long baseIndex = chunkStart + i;

                // Skip if no active profiles use this base vector
                boolean anyProfileActive = false;
                for (ProfileState profile : activeProfiles) {
                    if (baseIndex >= profile.baseRangeStart && baseIndex < profile.baseRangeEnd) {
                        anyProfileActive = true;
                        break;
                    }
                }

                if (!anyProfileActive) {
                    continue;
                }

                float[] baseVector = chunkVectors[i];

                // Update neighborhoods for all active profiles
                for (ProfileState profile : activeProfiles) {
                    if (baseIndex >= profile.baseRangeStart && baseIndex < profile.baseRangeEnd) {
                        updateProfileNeighborhoods(profile, baseIndex, baseVector);
                    }
                }

                // Progress within chunk (every 5 seconds)
                long now = System.currentTimeMillis();
                if (now - lastProgressReport >= 5000) {
                    double chunkPercent = (100.0 * (i + 1)) / chunkVectors.length;
                    logger.info("  Chunk {}: {}/{} vectors ({} %)",
                        chunkNumber, i + 1, chunkVectors.length,
                        String.format("%.1f", chunkPercent));
                    lastProgressReport = now;
                }
            }

            // After each chunk, sort and trim the buffers to keep only top K
            for (ProfileState profile : activeProfiles) {
                trimProfileBuffers(profile);
            }

            // Check if any profiles completed after this chunk
            Iterator<ProfileState> iterator = activeProfiles.iterator();
            while (iterator.hasNext()) {
                ProfileState profile = iterator.next();
                if (chunkEnd >= profile.baseRangeEnd) {
                    iterator.remove();
                    completedProfiles.add(profile);
                    logger.info("Profile '{}' completed processing {} base vectors, verifying {} queries...",
                        profile.profileName, profile.baseRangeEnd - profile.baseRangeStart,
                        profile.queryRangeEnd - profile.queryRangeStart);

                    int errors = finalizeAndVerifyProfile(profile);
                    totalErrors += errors;

                    if (totalErrors > 0 && errorMode == ErrorMode.Fail) {
                        return totalErrors;
                    }
                }
            }

            // Progress logging
            long processed = chunkEnd - minBaseIndex;
            long total = maxBaseIndex - minBaseIndex;
            double percent = (100.0 * processed) / total;
            String prefetchStatus = nextChunkFuture != null ? "1 buffering" : "none";
            logger.info("Progress: {}/{} base vectors ({} %) | Active: {} | Completed: {} | Errors: {} | Prefetch: {}",
                processed, total, String.format("%.1f", percent),
                activeProfiles.size(), completedProfiles.size(), totalErrors,
                prefetchStatus);
        }

        // Finalize any remaining active profiles (shouldn't happen, but handle it)
        for (ProfileState profile : activeProfiles) {
            logger.info("Finalizing remaining profile '{}'...", profile.profileName);
            int errors = finalizeAndVerifyProfile(profile);
            totalErrors += errors;

            if (totalErrors > 0 && errorMode == ErrorMode.Fail) {
                break;
            }
        }

        return totalErrors;
    }

    /**
     * Update all query neighborhoods for a profile with the current base vector
     */
    private void updateProfileNeighborhoods(ProfileState profile, long baseIndex, float[] baseVector) {
        // Use preloaded query vectors for maximum performance
        for (int queryIdx = 0; queryIdx < profile.knnBuffers.size(); queryIdx++) {
            float[] queryVector = profile.queryVectorArray[queryIdx];

            double distance = profile.distanceFunction.distance(queryVector, baseVector);

            List<NeighborIndex> buffer = profile.knnBuffers.get(queryIdx);
            buffer.add(new NeighborIndex(baseIndex, distance));
        }
    }

    /**
     * Trim profile buffers to keep only the top K + buffer_limit neighbors
     * This prevents memory from growing unbounded
     */
    private void trimProfileBuffers(ProfileState profile) {
        int keepSize = profile.effectiveK + buffer_limit;

        for (List<NeighborIndex> buffer : profile.knnBuffers) {
            if (buffer.size() > keepSize) {
                // Sort by distance, then by index for stable tie-breaking
                buffer.sort(Comparator.comparingDouble(NeighborIndex::distance)
                    .thenComparingLong(NeighborIndex::index));
                // Remove elements beyond keepSize
                buffer.subList(keepSize, buffer.size()).clear();
            }
        }
    }

    private int computeBufferLimit(int totalVectors) {
        if (buffer_limit > 0) {
            return buffer_limit;
        }
        int limit = 10;
        while (limit * 10 < totalVectors && limit < 100000) {
            limit *= 10;
        }
        return limit;
    }

    /**
     * Finalize and verify a single profile
     */
    private int finalizeAndVerifyProfile(ProfileState profile) {
        int errors = 0;

        logger.info("Verifying {} queries for profile '{}' (K={})",
            profile.knnBuffers.size(), profile.profileName, profile.effectiveK);

        RangeOption.Range queryRange = new RangeOption.Range(
            profile.queryRangeStart, profile.queryRangeEnd);

        try (StatusView view = getStatusView(queryRange)) {
            view.onStart((int)queryRange.size());

            for (int queryIdx = 0; queryIdx < profile.knnBuffers.size(); queryIdx++) {
                long actualQueryIndex = profile.queryRangeStart + queryIdx;

                // Get the query vector
                Indexed<float[]> query = profile.queryVectors.getIndexed(actualQueryIndex);
                view.onQueryVector(query, actualQueryIndex, profile.queryRangeEnd);

                // Get the provided neighborhood from the indices file
                int[] providedNeighborhood = profile.neighborIndices.get(actualQueryIndex);

                // Compute the expected neighborhood from the KNN buffer
                List<NeighborIndex> knnBuffer = profile.knnBuffers.get(queryIdx);
                int[] expectedNeighborhood = computeNeighborhoodFromBuffer(knnBuffer, profile.effectiveK);

                // Compare neighborhoods
                NeighborhoodComparison comparison =
                    new NeighborhoodComparison(query, providedNeighborhood, expectedNeighborhood);
                view.onNeighborhoodComparison(comparison);

                if (comparison.isError()) {
                    errors++;
                    logger.error("Profile '{}' query {}: {}", profile.profileName, actualQueryIndex, comparison.toString().trim());
                    System.err.println("  Provided (from file): " + java.util.Arrays.toString(providedNeighborhood));
                    System.err.println("  Expected (computed):  " + java.util.Arrays.toString(expectedNeighborhood));
                    // Show first few buffer entries with distances
                    System.err.println("  Top " + Math.min(15, knnBuffer.size()) + " buffer entries:");
                    for (int b = 0; b < Math.min(15, knnBuffer.size()); b++) {
                        NeighborIndex ni = knnBuffer.get(b);
                        System.err.println(String.format("    [%d] index=%d, distance=%.8f", b, ni.index(), ni.distance()));
                    }

                    if (errorMode == ErrorMode.Fail) {
                        break;
                    }
                }
            }

            view.end();
        } catch (Exception e) {
            logger.error("Error verifying profile '{}': {}", profile.profileName, e.getMessage());
            throw new RuntimeException("Error verifying profile: " + profile.profileName, e);
        }

        if (errors == 0) {
            logger.info("✓ Profile '{}': PASS (all {} queries verified correctly)",
                profile.profileName, profile.knnBuffers.size());
        } else {
            logger.error("✗ Profile '{}': FAIL ({} error{} in {} queries)",
                profile.profileName, errors, errors == 1 ? "" : "s", profile.knnBuffers.size());
        }

        return errors;
    }

    /**
     * Compute the final K-nearest neighbors from the accumulated buffer
     */
    private int[] computeNeighborhoodFromBuffer(List<NeighborIndex> buffer, int k) {
        // Sort by distance, then by index for stable tie-breaking
        buffer.sort(Comparator.comparingDouble(NeighborIndex::distance)
            .thenComparingLong(NeighborIndex::index));

        int[] neighborhood = new int[Math.min(k, buffer.size())];
        for (int i = 0; i < neighborhood.length; i++) {
            neighborhood[i] = (int) buffer.get(i).index();
        }

        // Check for ties at the K boundary
        if (buffer.size() > k) {
            double kthDistance = buffer.get(k - 1).distance();
            int tieCount = 0;
            for (int i = k; i < buffer.size(); i++) {
                if (Math.abs(buffer.get(i).distance() - kthDistance) < phi) {
                    tieCount++;
                }
            }
            if (tieCount > 0) {
                logger.warn("Tie at K={} boundary: {} additional neighbors have distance within phi={} of the Kth neighbor (distance={})",
                    k, tieCount, phi, kthDistance);
            }
        }

        return neighborhood;
    }

    /**
     * Validate user-specified K against detected K and return the effective K to use
     */
    private int validateAndSetNeighborhoodSize(int detectedK) {
        if (K == -1) {
            logger.info("Auto-setting neighborhood size to K={}", detectedK);
            return detectedK;
        } else if (K > detectedK) {
            throw new IllegalArgumentException(
                "Specified neighborhood size (K=" + K + ") exceeds the neighborhood size " +
                    "in the indices file (K=" + detectedK + "). The indices file only contains " +
                    detectedK + " neighbors per query."
            );
        } else if (K < detectedK) {
            logger.info("Using user-specified K={} (indices file contains K={}, will verify first {} neighbors only)",
                K, detectedK, K);
            return K;
        } else {
            logger.info("Using neighborhood size K={} (matches indices file)", K);
            return K;
        }
    }

    private StatusView getStatusView(RangeOption.Range range) {
        @SuppressWarnings("resource") StatusViewRouter view = new StatusViewRouter();
        switch (output) {
            case All:
            case Progress:
                try {
                    view.add(new StatusViewLanterna(Math.min(3, (int)range.size())));
                } catch (Exception e) {
                    // If Lanterna cannot initialize (no TTY, headless), fall back to stdout only
                    output = StatusMode.Stdout;
                }
                break;
            default:
                break;
        }
        switch (output) {
            case All:
            case Stdout:
                view.add(new StatusViewStdout(view.isEmpty()));
                break;
            default:
                break;
        }
        return view.isEmpty() ? new StatusViewNoOp() : view;
    }

    /**
     * Internal class to track the state of a single profile during verification
     */
    private static class ProfileState {
        String profileName;
        TestDataView profile;
        FloatVectors baseVectors;
        FloatVectors queryVectors;
        IntVectors neighborIndices;
        DistanceFunction distanceFunction;

        long baseRangeStart;
        long baseRangeEnd;
        long queryRangeStart;
        long queryRangeEnd;

        int effectiveK;

        // KNN buffers for each query in the range
        List<List<NeighborIndex>> knnBuffers;

        // Preloaded query vectors for performance
        float[][] queryVectorArray;
    }
}
