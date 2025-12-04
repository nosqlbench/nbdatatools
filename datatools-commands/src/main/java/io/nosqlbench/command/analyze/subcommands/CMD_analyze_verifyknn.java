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
import io.nosqlbench.command.analyze.subcommands.verify_knn.options.ConsoleDiagnostics;
import io.nosqlbench.command.analyze.subcommands.verify_knn.options.ErrorMode;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusMode;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusView;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewLanterna;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewNoOp;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewRouter;
import io.nosqlbench.command.analyze.subcommands.verify_knn.statusview.StatusViewStdout;
import io.nosqlbench.command.common.CommandLineFormatter;
import io.nosqlbench.command.common.RangeOption;
import io.nosqlbench.command.common.BaseVectorsInputFileOption;
import io.nosqlbench.command.common.QueryVectorsInputFileOption;
import io.nosqlbench.command.common.IndicesInputFileOption;
import io.nosqlbench.command.common.DistancesInputFileOption;
import io.nosqlbench.vectordata.discovery.DatasetLoader;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.IntVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// This app will read data from vector dataset files (HDF5, FVecs, IVecs, etc.) in the
/// standard vector KNN answer key format, computing correct neighborhoods and comparing
/// them to the provided ones.
///
/// Internally, values may be promoted from int to long and from float to double as needed.
/// For now, floats are used by default, since that is the precision in current test files.
@CommandLine.Command(name = "verify_knn",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "self-check KNN test data answer-keys",
    description = "Reads query vectors from dataset files (HDF5, dataset.yaml with xvec files, or remote URLs),\n" +
        "computes KNN neighborhoods, and compares them against the answer-key data given.\n" +
        "This is a pure Java implementation which requires no other vector processing\n" +
        "libraries or hardware, so it has two key trade-offs with other methods:\n" +
        "1. It is not as fast as a GPU or TPU. It is not expected to be.\n" +
        "2. It is a vastly simpler implementation, which makes it arguably easier\n" +
        "   to rely on as a basic verification tool.\n" +
        "This utility is meant to be used in concert with other tools which are\n" +
        "faster, but which may benefit from the assurance of a basic coherence check.\n" +
        "In essence, if you are not sure your test data is self-correct, then use\n" +
        "this tool to double check it with some sparse sampling.\n\n" +
        "The currently supported distance functions and file formats are indicated\n" +
        "by the available command line options.\n\n" +
        "Supports loading datasets from:\n" +
        "- Local HDF5 files\n" +
        "- Local directories with dataset.yaml (xvec format)\n" +
        "- Remote URLs (with automatic caching)\n" +
        "- Explicit individual files (--base, --query, --indices)\n\n" +
        "The pseudo-standard HDF5 KNN answer-key file format is documented here:\n" +
        "https://github.com/nosqlbench/nbdatatools/blob/main/nbvectors/src/docs/hdf5_vectors.md",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: all tested neighborhoods were correct",
        "2: at least one tested neighborhood was incorrect"
    })
public class CMD_analyze_verifyknn implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(CMD_analyze_verifyknn.class);

  @CommandLine.Mixin
  private RangeOption rangeOption = new RangeOption();

  @CommandLine.Mixin
  private BaseVectorsInputFileOption baseVectorsOption = new BaseVectorsInputFileOption();

  @CommandLine.Mixin
  private QueryVectorsInputFileOption queryVectorsOption = new QueryVectorsInputFileOption();

  @CommandLine.Mixin
  private IndicesInputFileOption indicesOption = new IndicesInputFileOption();

  @CommandLine.Mixin
  private DistancesInputFileOption distancesOption = new DistancesInputFileOption();

  @Parameters(description = "The dataset file(s), directory, or URL to load (supports HDF5, dataset.yaml, and remote URLs; defaults to current directory if no explicit files specified)", arity = "0..*")
  private List<Path> hdfpaths = new ArrayList<>();

  @Option(names = {"-d", "--distance_function"},
      defaultValue = "COSINE",
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
      defaultValue = "all",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private StatusMode output;

  @Option(names = {"-e", "--error_mode"},
      defaultValue = "fail",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private ErrorMode errorMode;

  @Option(names = {"-p", "--phi"}, defaultValue = "0.001d", description = "When comparing values which are not exact, due to floating point rounding\n" +
      "errors, the distance within which the values are considered effectively\n" +
      "the same.")
  private double phi;

  @Option(names = {"--profile", "--config"},
      description = "The config profile to use from the dataset. (default ${DEFAULT-VALUE})",
      defaultValue = "ALL")
  private String dataconfig = "ALL";

  @Option(names = {"--_diaglevel", "-_d"}, hidden = true, description = "Internal diagnostic level, sends content directly to the console.", defaultValue = "ERROR")
  ConsoleDiagnostics diaglevel;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    // Print the command line being executed
    CommandLineFormatter.printCommandLine(spec);
    logger.info("");

    int errors = 0;
    try {
      // Detect which mode we're in
      boolean explicitFileMode = isExplicitFileMode();

      if (explicitFileMode) {
        // Explicit file mode: user specified individual files
        logger.info("Using explicit file mode");
        errors = verifyExplicitFiles();
      } else {
        // Auto-discovery mode: discover files from directory
        logger.info("Using auto-discovery mode");
        errors = verifyAutoDiscovery();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return errors > 0 ? 2 : 0;
  }

  /**
   * Checks if we're in explicit file mode (user specified individual files)
   */
  private boolean isExplicitFileMode() {
    return baseVectorsOption.isSpecified() ||
           queryVectorsOption.isSpecified() ||
           indicesOption.isSpecified();
  }

  /**
   * Validates that all required files are specified for explicit mode
   */
  private void validateExplicitFileMode() {
    List<String> missing = new ArrayList<>();

    if (!baseVectorsOption.isSpecified()) {
      missing.add("--base (base vectors)");
    }
    if (!queryVectorsOption.isSpecified()) {
      missing.add("--query (query vectors)");
    }
    if (!indicesOption.isSpecified()) {
      missing.add("--indices (neighbor indices)");
    }

    if (!missing.isEmpty()) {
      throw new IllegalArgumentException(
        "Explicit file mode requires all of: --base, --query, --indices. Missing: " +
        String.join(", ", missing));
    }

    // Validate files exist
    baseVectorsOption.validateBaseVectors();
    queryVectorsOption.validateQueryVectors();
    indicesOption.validateIndicesInput();
    if (distancesOption.isSpecified()) {
      distancesOption.validateDistancesInput();
    }
  }

  /**
   * Verify using explicit file paths
   */
  private int verifyExplicitFiles() throws Exception {
    validateExplicitFileMode();

    int errors = 0;

    // Load vectors from files using the existing XvecImpl classes with MAFileChannel
    Path basePath = baseVectorsOption.getNormalizedBasePath();
    Path queryPath = queryVectorsOption.getNormalizedQueryPath();
    Path indicesPath = indicesOption.getNormalizedIndicesPath();

    String baseRange = baseVectorsOption.getInlineRange();
    String queryRange = queryVectorsOption.getInlineRange();
    String indicesRange = indicesOption.getInlineRange();

    logger.info("Loading base vectors from: {}{}", basePath,
        baseRange != null ? ":" + baseRange : "");
    FloatVectorsWithOffset baseVectorsWithOffset = loadFloatVectors(basePath, baseRange);
    FloatVectors baseVectors = baseVectorsWithOffset.vectors;
    long globalOffset = baseVectorsWithOffset.globalOffset;

    logger.info("Loading query vectors from: {}{}", queryPath,
        queryRange != null ? ":" + queryRange : "");
    FloatVectorsWithOffset queryVectorsWithOffset = loadFloatVectors(queryPath, queryRange);
    FloatVectors queryVectors = queryVectorsWithOffset.vectors;

    logger.info("Loading neighbor indices from: {}{}", indicesPath,
        indicesRange != null ? ":" + indicesRange : "");
    IntVectors indices = loadIntVectors(indicesPath, indicesRange);

    logger.info("Loaded vectors - base: {}, query: {}, indices: {}",
        baseVectors.getCount(), queryVectors.getCount(), indices.getCount());

    // Auto-detect and validate neighborhood size
    int detectedK = detectNeighborhoodSize(indices);
    int effectiveK = validateAndSetNeighborhoodSize(detectedK);

    // Get effective range: default to first query if not specified
    RangeOption.Range effectiveRange = rangeOption.isRangeSpecified()
        ? rangeOption.getRange()
        : new RangeOption.Range(0, 1);

    // Validate range against available data
    if (effectiveRange.end() > queryVectors.getCount()) {
      throw new IllegalArgumentException(
          "Range end " + effectiveRange.end() +
          " exceeds query vector count " + queryVectors.getCount()
      );
    }

    try (StatusView view = getStatusView(effectiveRange)) {
      view.onStart((int)effectiveRange.size());

      for (long index = effectiveRange.start(); index < effectiveRange.end(); index++) {
        // Get the query vector
        Indexed<float[]> query = queryVectors.getIndexed(index);
        view.onQueryVector(query, index, effectiveRange.end());

        // Get the provided neighborhood from the indices file
        int[] providedNeighborhood = indices.get(index);

        // Compute the expected neighborhood using the global offset from the base vectors
        int[] expectedNeighborhood = computeNeighborhood(query, baseVectors, view, effectiveK, globalOffset);

        // Compare neighborhoods
        NeighborhoodComparison comparison =
            new NeighborhoodComparison(query, providedNeighborhood, expectedNeighborhood);
        view.onNeighborhoodComparison(comparison);
        errors += comparison.isError() ? 1 : 0;

        if (errors > 0 && errorMode == ErrorMode.Fail) {
          break;
        }
      }

      view.end();
    }

    return errors;
  }

  /**
   * Verify using auto-discovery from directory
   */
  private int verifyAutoDiscovery() throws Exception {
    int errors = 0;

    // If no paths provided, default to current directory
    List<Path> pathsToProcess = hdfpaths.isEmpty() ? List.of(Path.of(".")) : hdfpaths;

    for (Path hdfpath : pathsToProcess) {

      ProfileSelector datag;
      TestDataView data;

      try {
        datag = DatasetLoader.load(hdfpath);
        data = datag.profile("default");
      } catch (Exception e) {
        logger.error("Failed to load dataset from {}: {}", hdfpath, e.getMessage());
        logger.error("Auto-discovery requires HDF5 file, directory with dataset.yaml, or compatible data files");
        logger.error("For explicit file mode, use: --base <file> --query <file> --indices <file>");
        return 1;
      }

      try (ProfileSelector ignored = datag) {

          List<String> configs = new ArrayList<>();
          configs.add(this.dataconfig);
          for (String config : configs) {

            logger.info("loaded vector data file: {}", data.toString());

            Optional<NeighborDistances> distances = data.getNeighborDistances();

            if (data.getNeighborDistances().isEmpty()) {
              logger.error("neighbor distances are not available in the provided data, so "
                           + "distance-based verification is not possible.");
              return 1;
            } else {
              logger.info("loaded neighbor distances: {}", distances.get().toString());
            }
            IntVectors indices = (IntVectors) data.getNeighborIndices()
                .orElseThrow(() -> new RuntimeException("Neighbor indices are not available int he provided data, so distance-based verification is not possible."));
            FloatVectors baseVectors =
                data.getBaseVectors().orElseThrow(() -> new RuntimeException("Base vectors are not available in the provided data, so distance-based verification is not possible."));
            FloatVectors queryVectors =
                data.getQueryVectors().orElseThrow(() -> new RuntimeException("Query vectors are not available in the provided data, so distance-based verification is not possible."));

            if (baseVectors instanceof FloatVectors) {
              logger.info("loaded base vectors: {}", baseVectors.toString());
            } else {
              throw new RuntimeException("unsupported vector type: " + baseVectors.getClass());
            }

            // Auto-detect and validate neighborhood size
            int detectedK = detectNeighborhoodSize(indices);
            int effectiveK = validateAndSetNeighborhoodSize(detectedK);

            // Get effective range: default to first query if not specified
            RangeOption.Range effectiveRange = rangeOption.isRangeSpecified()
                ? rangeOption.getRange()
                : new RangeOption.Range(0, 1);

            try (StatusView view = getStatusView(effectiveRange)) {
              view.onStart((int)effectiveRange.size());
            for (long index = effectiveRange.start(); index < effectiveRange.end(); index++) {

              // This is the query vector from the provided test data
              Indexed<float[]> query = queryVectors.getIndexed(index);

              view.onQueryVector(query, index, effectiveRange.end());

              // This is the neighborhood from the provided test data, corresponding to the query
              // vector (we are checking this one for errors)
              int[] providedNeighborhood = indices.get(index);

              int[] expectedNeighborhood;
              // This neighborhood is the one we calculate from the test and train vectors.
              // Auto-discovery mode always uses full base vectors (no offset)
              expectedNeighborhood = computeNeighborhood(query, baseVectors, view, effectiveK, 0);

              // Compute the ordered intersection view of these relative to each other
              NeighborhoodComparison comparison =
                  new NeighborhoodComparison(query, providedNeighborhood, expectedNeighborhood);
              view.onNeighborhoodComparison(comparison);
              errors += comparison.isError() ? 1 : 0;
              if (errors > 0 && errorMode == ErrorMode.Fail)
                break;
            }
              view.end();
            }
          }
        }

      }

    return errors;
  }

  private StatusView getStatusView(RangeOption.Range range) {
    @SuppressWarnings("resource") StatusViewRouter view = new StatusViewRouter();
    switch (output) {
      case All:
      case Progress:
        view.add(new StatusViewLanterna(Math.min(3, (int)range.size())));
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


  private int[] computeNeighborhood(
      Indexed<float[]> testVector,
      FloatVectors baseVectors,
      StatusView view,
      int k,
      long globalIndexOffset
  )
  {
    int count = baseVectors.getCount();
    buffer_limit = buffer_limit > 0 ? buffer_limit : computeBufferLimit(count);
    float[] testVecAry = testVector.value();
    int totalTrainingVectors = count;

    NeighborIndex[] topKResultBuffer = new NeighborIndex[0];
    for (int chunk = 0; chunk < totalTrainingVectors; chunk += buffer_limit) {
      // do a whole chunk, or a partial if that is all that remains
      int chunkSize = Math.min(chunk + buffer_limit, totalTrainingVectors) - chunk;
      // buffer topK + chunkSize neighbors with distance
      NeighborIndex[] unsortedNeighbors = new NeighborIndex[chunkSize + topKResultBuffer.length];
      // but include previous results at the max, so buffer addressing remains 0+...
      System.arraycopy(topKResultBuffer, 0, unsortedNeighbors, chunkSize, topKResultBuffer.length);
      view.onChunk(chunk, chunkSize, totalTrainingVectors);
      // fill the unordered neighborhood with the next batch of vector ordinals and distances
      for (int i = 0; i < chunkSize; i++) {
        int localIndex = chunk + i;
        long globalIndex = globalIndexOffset + localIndex;
        float[] trainVector = baseVectors.get(localIndex);
        double distance = distanceFunction.distance(testVecAry, trainVector);
        unsortedNeighbors[i] = new NeighborIndex(globalIndex, distance);
      }

      // put the neighborhood in order and keep the top K results
      Arrays.sort(unsortedNeighbors, Comparator.comparing(NeighborIndex::distance));
      topKResultBuffer = new NeighborIndex[k];
      System.arraycopy(unsortedNeighbors, 0, topKResultBuffer, 0, topKResultBuffer.length);
    }
    int[] neighborhood = new int[topKResultBuffer.length];
    for (int i = 0; i < neighborhood.length; i++) {
      neighborhood[i] = (int) topKResultBuffer[i].index();
    }
    return neighborhood;
  }

  /**
   * Detect the neighborhood size (K) from the indices file
   * @param indices The neighbor indices dataset
   * @return The detected K value (number of neighbors per query)
   */
  private int detectNeighborhoodSize(IntVectors indices) {
    if (indices.getCount() == 0) {
      throw new IllegalArgumentException("Indices file is empty, cannot detect neighborhood size");
    }

    // Read the first index vector to determine K
    int[] firstNeighborhood = indices.get(0);
    int detectedK = firstNeighborhood.length;

    logger.info("Detected neighborhood size (K) from indices file: {}", detectedK);
    return detectedK;
  }

  /**
   * Validate user-specified K against detected K and return the effective K to use
   * @param detectedK The K value detected from the indices file
   * @return The effective K value to use for verification
   * @throws IllegalArgumentException if user-specified K is larger than detected K
   */
  private int validateAndSetNeighborhoodSize(int detectedK) {
    if (K == -1) {
      // Auto-set K from detected value
      logger.info("Auto-setting neighborhood size to K={}", detectedK);
      return detectedK;
    } else if (K > detectedK) {
      // User specified K larger than what's in the file
      throw new IllegalArgumentException(
          "Specified neighborhood size (K=" + K + ") exceeds the neighborhood size " +
          "in the indices file (K=" + detectedK + "). The indices file only contains " +
          detectedK + " neighbors per query."
      );
    } else if (K < detectedK) {
      // User specified smaller K - this is valid, we'll only check the first K neighbors
      logger.info("Using user-specified K={} (indices file contains K={}, will verify first {} neighbors only)",
          K, detectedK, K);
      return K;
    } else {
      // User specified K matches detected K
      logger.info("Using neighborhood size K={} (matches indices file)", K);
      return K;
    }
  }

  private int computeBufferLimit(int totalTrainingVectors) {
    int limit = 10;
    while (limit * 10 < totalTrainingVectors && limit < 100000) {
      limit *= 10;
    }
    return limit;
  }

  /**
   * Load float vectors from a file using the xvec implementation directly
   * @param path The path to the vector file
   * @param rangeSpec Optional range specification (e.g., "1000", "[0,1000)", "0..1000")
   */
  private FloatVectorsWithOffset loadFloatVectors(Path path, String rangeSpec) throws IOException {
    // Parse range if specified
    RangeOption.Range range = null;
    if (rangeSpec != null && !rangeSpec.isEmpty()) {
      range = new RangeOption.RangeConverter().convert(rangeSpec);
    }

    // Get file extension
    String fileName = path.getFileName().toString();
    String extension = fileName.substring(fileName.lastIndexOf('.') + 1);

    // Open file channel and create FloatVectorsXvecImpl directly
    java.nio.channels.AsynchronousFileChannel channel =
        java.nio.channels.AsynchronousFileChannel.open(path, java.nio.file.StandardOpenOption.READ);
    long fileSize = java.nio.file.Files.size(path);

    io.nosqlbench.vectordata.spec.datasets.impl.xvec.FloatVectorsXvecImpl baseVectors =
        new io.nosqlbench.vectordata.spec.datasets.impl.xvec.FloatVectorsXvecImpl(
            channel, fileSize, null, extension);

    // Wrap with range support if needed
    FloatVectors vectors = range != null
        ? new RangedFloatVectors(baseVectors, range)
        : baseVectors;

    long globalOffset = (range != null) ? range.start() : 0;
    return new FloatVectorsWithOffset(vectors, globalOffset);
  }

  /**
   * Load int vectors from a file using the xvec implementation directly
   * @param path The path to the vector file
   * @param rangeSpec Optional range specification (e.g., "1000", "[0,1000)", "0..1000")
   */
  private IntVectors loadIntVectors(Path path, String rangeSpec) throws IOException {
    // Parse range if specified
    RangeOption.Range range = null;
    if (rangeSpec != null && !rangeSpec.isEmpty()) {
      range = new RangeOption.RangeConverter().convert(rangeSpec);
    }

    // Get file extension
    String fileName = path.getFileName().toString();
    String extension = fileName.substring(fileName.lastIndexOf('.') + 1);

    // Open file channel and create IntVectorsXvecImpl directly
    java.nio.channels.AsynchronousFileChannel channel =
        java.nio.channels.AsynchronousFileChannel.open(path, java.nio.file.StandardOpenOption.READ);
    long fileSize = java.nio.file.Files.size(path);

    io.nosqlbench.vectordata.spec.datasets.impl.xvec.IntVectorsXvecImpl baseVectors =
        new io.nosqlbench.vectordata.spec.datasets.impl.xvec.IntVectorsXvecImpl(
            channel, fileSize, null, extension);

    // Wrap with range support if needed
    return range != null
        ? new RangedIntVectors(baseVectors, range)
        : baseVectors;
  }

  /**
   * Helper class to track FloatVectors along with its global offset
   */
  private static class FloatVectorsWithOffset {
    final FloatVectors vectors;
    final long globalOffset;

    FloatVectorsWithOffset(FloatVectors vectors, long globalOffset) {
      this.vectors = vectors;
      this.globalOffset = globalOffset;
    }
  }

  /**
   * Wrapper to add range support to FloatVectors implementations
   */
  private static class RangedFloatVectors implements FloatVectors {
    private final FloatVectors delegate;
    private final RangeOption.Range range;

    RangedFloatVectors(FloatVectors delegate, RangeOption.Range range) {
      this.delegate = delegate;
      this.range = range;
    }

    @Override
    public float[] get(long index) {
      return delegate.get(range.start() + index);
    }

    @Override
    public Indexed<float[]> getIndexed(long index) {
      return new Indexed<>(index, get(index));
    }

    @Override
    public int getCount() {
      return (int)Math.min(range.size(), delegate.getCount() - range.start());
    }

    @Override
    public int getVectorDimensions() {
      return delegate.getVectorDimensions();
    }

    @Override
    public Class<?> getDataType() {
      return delegate.getDataType();
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
      return delegate.prebuffer(range.start() + startIncl, range.start() + endExcl);
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> prebuffer() {
      return delegate.prebuffer(range.start(), range.end());
    }

    @Override
    public java.util.concurrent.Future<float[]> getAsync(long index) {
      return delegate.getAsync(range.start() + index);
    }

    @Override
    public float[][] getRange(long startInclusive, long endExclusive) {
      return delegate.getRange(range.start() + startInclusive, range.start() + endExclusive);
    }

    @Override
    public java.util.concurrent.Future<float[][]> getRangeAsync(long startInclusive, long endExclusive) {
      return delegate.getRangeAsync(range.start() + startInclusive, range.start() + endExclusive);
    }

    @Override
    public java.util.concurrent.Future<Indexed<float[]>> getIndexedAsync(long index) {
      return java.util.concurrent.CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<float[]>[] getIndexedRange(long startInclusive, long endExclusive) {
      float[][] vectors = getRange(startInclusive, endExclusive);
      Indexed<float[]>[] result = new Indexed[(int)(endExclusive - startInclusive)];
      for (int i = 0; i < result.length; i++) {
        result[i] = new Indexed<>(startInclusive + i, vectors[i]);
      }
      return result;
    }

    @Override
    public java.util.concurrent.Future<Indexed<float[]>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
      return java.util.concurrent.CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public List<float[]> toList() {
      List<float[]> result = new ArrayList<>(getCount());
      for (int i = 0; i < getCount(); i++) {
        result.add(get(i));
      }
      return result;
    }

    @Override
    public <U> List<U> toList(java.util.function.Function<float[], U> f) {
      List<U> result = new ArrayList<>(getCount());
      for (int i = 0; i < getCount(); i++) {
        result.add(f.apply(get(i)));
      }
      return result;
    }

    @Override
    public java.util.Iterator<float[]> iterator() {
      return new java.util.Iterator<>() {
        private int index = 0;

        @Override
        public boolean hasNext() {
          return index < getCount();
        }

        @Override
        public float[] next() {
          return get(index++);
        }
      };
    }
  }

  /**
   * Wrapper to add range support to IntVectors implementations
   */
  private static class RangedIntVectors implements IntVectors {
    private final IntVectors delegate;
    private final RangeOption.Range range;

    RangedIntVectors(IntVectors delegate, RangeOption.Range range) {
      this.delegate = delegate;
      this.range = range;
    }

    @Override
    public int[] get(long index) {
      return delegate.get(range.start() + index);
    }

    @Override
    public Indexed<int[]> getIndexed(long index) {
      return new Indexed<>(index, get(index));
    }

    @Override
    public int getCount() {
      return (int)Math.min(range.size(), delegate.getCount() - range.start());
    }

    @Override
    public int getVectorDimensions() {
      return delegate.getVectorDimensions();
    }

    @Override
    public Class<?> getDataType() {
      return delegate.getDataType();
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
      return delegate.prebuffer(range.start() + startIncl, range.start() + endExcl);
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> prebuffer() {
      return delegate.prebuffer(range.start(), range.end());
    }

    @Override
    public java.util.concurrent.Future<int[]> getAsync(long index) {
      return delegate.getAsync(range.start() + index);
    }

    @Override
    public int[][] getRange(long startInclusive, long endExclusive) {
      return delegate.getRange(range.start() + startInclusive, range.start() + endExclusive);
    }

    @Override
    public java.util.concurrent.Future<int[][]> getRangeAsync(long startInclusive, long endExclusive) {
      return delegate.getRangeAsync(range.start() + startInclusive, range.start() + endExclusive);
    }

    @Override
    public java.util.concurrent.Future<Indexed<int[]>> getIndexedAsync(long index) {
      return java.util.concurrent.CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<int[]>[] getIndexedRange(long startInclusive, long endExclusive) {
      int[][] vectors = getRange(startInclusive, endExclusive);
      Indexed<int[]>[] result = new Indexed[(int)(endExclusive - startInclusive)];
      for (int i = 0; i < result.length; i++) {
        result[i] = new Indexed<>(startInclusive + i, vectors[i]);
      }
      return result;
    }

    @Override
    public java.util.concurrent.Future<Indexed<int[]>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
      return java.util.concurrent.CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public List<int[]> toList() {
      List<int[]> result = new ArrayList<>(getCount());
      for (int i = 0; i < getCount(); i++) {
        result.add(get(i));
      }
      return result;
    }

    @Override
    public <U> List<U> toList(java.util.function.Function<int[], U> f) {
      List<U> result = new ArrayList<>(getCount());
      for (int i = 0; i < getCount(); i++) {
        result.add(f.apply(get(i)));
      }
      return result;
    }

    @Override
    public List<java.util.Set<Integer>> asSets() {
      List<java.util.Set<Integer>> sets = new ArrayList<>(getCount());
      for (int[] vector : this) {
        java.util.LinkedHashSet<Integer> set = new java.util.LinkedHashSet<>(vector.length);
        for (int value : vector) {
          set.add(value);
        }
        sets.add(set);
      }
      return sets;
    }

    @Override
    public java.util.Iterator<int[]> iterator() {
      return new java.util.Iterator<>() {
        private int index = 0;

        @Override
        public boolean hasNext() {
          return index < getCount();
        }

        @Override
        public int[] next() {
          return get(index++);
        }
      };
    }
  }

}
