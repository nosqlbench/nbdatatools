package io.nosqlbench.vectordata.downloader;

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


import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.datasets.types.ViewKind;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.BaseVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborDistancesXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborIndicesXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.QueryVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.tokens.SpecToken;
import io.nosqlbench.vectordata.spec.tokens.Templatizer;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.ArrayList;

import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture;
import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicator;

/// Implementation of TestDataView that provides access to vector datasets and their metadata.
///
/// VirtualTestDataView serves as a bridge between dataset descriptions (metadata) and the actual
/// vector data. It handles both local and remote data sources, using appropriate file access
/// mechanisms based on the source URL protocol. For remote resources, it uses a Merkle tree-based
/// approach for efficient access and verification.
///
/// The class provides methods to access:
/// - Base vectors (the core dataset vectors)
/// - Query vectors (vectors used for querying/testing)
/// - Neighbor indices and distances (ground truth for nearest neighbor search)
/// - Metadata like distance function, license, model, and vendor information
///
/// It also supports token-based templating for generating parameterized strings based on
/// dataset attributes.
///
/// ```
/// Dataset Access Flow:
///   DatasetEntry + DSProfile → VirtualTestDataView → Vector Data Access
///                                     ↓
///                           Local/Remote File Access
///                                     ↓
///                            Specialized Vector Views
/// ```
public class VirtualVectorTestDataView implements VectorTestDataView {

  private static final Logger logger = LogManager.getLogger(VirtualVectorTestDataView.class);

  /// The dataset entry containing metadata about the dataset
  private final DatasetEntry datasetEntry;

  /// The profile containing views of the dataset
  private final DSProfile profile;

  /// The directory where downloaded data is cached
  private Path cachedir = Path.of(System.getProperty("user.home"), ".cache", "vectordata");

  /// Cached views, initially backed by MAFileChannel, promoted to Path-based mmap
  /// after prebuffering completes.
  private volatile BaseVectors cachedBaseVectors;
  private volatile QueryVectors cachedQueryVectors;
  private volatile NeighborIndices cachedNeighborIndices;
  private volatile NeighborDistances cachedNeighborDistances;
  private volatile NeighborIndices cachedFilteredNeighborIndices;
  private volatile NeighborDistances cachedFilteredNeighborDistances;

  /// Tracks the resolution metadata for each view kind, used by
  /// {@link #promoteToLocalViews()} to replace MAFileChannel-backed views
  /// with Path-based mmap views after prebuffering.
  private static class ViewResolution {
    final MAFileChannel channel;
    final Path cachePath;
    final DSWindow window;
    final String extension;
    ViewResolution(MAFileChannel channel, Path cachePath, DSWindow window, String extension) {
      this.channel = channel;
      this.cachePath = cachePath;
      this.window = window;
      this.extension = extension;
    }
  }
  private final Map<ViewKind, ViewResolution> viewResolutions = new HashMap<>();

  /// Creates a new VirtualTestDataView with the specified cache directory, dataset entry, and profile.
  ///
  /// @param cachedir 
  ///     The directory where downloaded data is cached
  /// @param datasetEntry 
  ///     The dataset entry containing metadata about the dataset
  /// @param profile 
  ///     The profile containing views of the dataset
  public VirtualVectorTestDataView(Path cachedir, DatasetEntry datasetEntry, DSProfile profile) {
    this.datasetEntry = datasetEntry;
    this.profile = profile;
    this.cachedir = cachedir;
  }

  /// Returns the base vectors dataset if available.
  ///
  /// Base vectors are the core dataset vectors that form the basis of the vector database.
  /// This method attempts to locate and load the base vectors from the dataset profile.
  ///
  /// @return An Optional containing the base vectors dataset, or empty if not available
  @Override
  public Optional<BaseVectors> getBaseVectors() {
    BaseVectors cached = cachedBaseVectors;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.base);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      BaseVectorsXvecImpl newview = new BaseVectorsXvecImpl(
          channel, channel.size(), window, extension);
      cachedBaseVectors = newview;
      viewResolutions.put(ViewKind.base, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Creates a MAFileChannel for both local and remote URLs.
  ///
  /// This method now uses MAFileChannel directly for both local and remote files,
  /// providing consistent behavior and thread-safe access with absolute positioning.
  ///
  /// @param contentPath 
  ///     The local path where the content should be cached
  /// @param sourceContentURL 
  ///     The URL of the source content
  /// @return A MAFileChannel for accessing the content
  /// @throws IOException If there is an error accessing the file
  protected MAFileChannel resolveMAFileChannel(Path contentPath, URL sourceContentURL)
      throws IOException
  {
    // Use MAFileChannel for both local and remote resources
    return new MAFileChannel(contentPath, contentPath.resolveSibling(contentPath.getFileName() + ".mrkl"), sourceContentURL.toString());
  }

  /// Finds a view in the profile that matches the given view kind or its synonyms.
  ///
  /// This method handles the case where a view might be named with a synonym
  /// of the standard view kind (e.g., "base" instead of "base_vectors").
  ///
  /// @param viewkind 
  ///     The kind of view to find (e.g., "base_vectors", "query_vectors")
  /// @return An Optional containing the matching view, or empty if not found
  private Optional<DSView> getMatchingView(ViewKind viewKind) {
    Set<String> allValidKindSynonyms = viewKind.getAllNames();

    for (String viewName : profile.keySet()) {
      String normalized = viewName.toLowerCase(Locale.ROOT);
      if (allValidKindSynonyms.contains(normalized)) {
        return Optional.of(profile.get(viewName));
      }

      DSView dsView = profile.get(viewName);
      if (dsView != null && dsView.getName() != null) {
        String dsViewName = dsView.getName().toLowerCase(Locale.ROOT);
        if (allValidKindSynonyms.contains(dsViewName)) {
          return Optional.of(dsView);
        }
      }
    }
    return Optional.empty();
  }

  /// Returns the query vectors dataset if available.
  ///
  /// Query vectors are used for testing and benchmarking vector search algorithms.
  /// This method attempts to locate and load the query vectors from the dataset profile.
  ///
  /// @return An Optional containing the query vectors dataset, or empty if not available
  @Override
  public Optional<QueryVectors> getQueryVectors() {
    QueryVectors cached = cachedQueryVectors;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.query);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      QueryVectorsXvecImpl newview = new QueryVectorsXvecImpl(
          channel, channel.size(), window, extension);
      cachedQueryVectors = newview;
      viewResolutions.put(ViewKind.query, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Returns the neighbor indices dataset if available.
  ///
  /// Neighbor indices represent the ground truth for nearest neighbor search,
  /// containing the indices of the nearest neighbors for each query vector.
  /// This method attempts to locate and load the neighbor indices from the dataset profile.
  ///
  /// @return An Optional containing the neighbor indices dataset, or empty if not available
  @Override
  public Optional<NeighborIndices> getNeighborIndices() {
    NeighborIndices cached = cachedNeighborIndices;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.indices);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      NeighborIndicesXvecImpl newview = new NeighborIndicesXvecImpl(
          channel, channel.size(), window, extension);
      cachedNeighborIndices = newview;
      viewResolutions.put(ViewKind.indices, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Returns the neighbor distances dataset if available.
  ///
  /// Neighbor distances represent the ground truth distances between query vectors
  /// and their nearest neighbors in the base vectors dataset.
  /// This method attempts to locate and load the neighbor distances from the dataset profile.
  ///
  /// @return An Optional containing the neighbor distances dataset, or empty if not available
  @Override
  public Optional<NeighborDistances> getNeighborDistances() {
    NeighborDistances cached = cachedNeighborDistances;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.neighbors);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      NeighborDistancesXvecImpl newview = new NeighborDistancesXvecImpl(
          channel, channel.size(), window, extension);
      cachedNeighborDistances = newview;
      viewResolutions.put(ViewKind.neighbors, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Returns the filtered neighbor indices dataset if available.
  ///
  /// Filtered neighbor indices contain the ground truth for filtered KNN search,
  /// pre-conditioned on matching metadata predicates. Unlike standard neighbor indices
  /// which are computed from pure KNN, these incorporate predicate filtering so that
  /// recall does not degrade arbitrarily.
  ///
  /// @return An Optional containing the filtered neighbor indices, or empty if not available
  @Override
  public Optional<NeighborIndices> getFilteredNeighborIndices() {
    NeighborIndices cached = cachedFilteredNeighborIndices;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.filtered_indices);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      NeighborIndicesXvecImpl newview = new NeighborIndicesXvecImpl(
          channel, channel.size(), window, extension);
      cachedFilteredNeighborIndices = newview;
      viewResolutions.put(ViewKind.filtered_indices, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Returns the filtered neighbor distances dataset if available.
  ///
  /// Filtered neighbor distances contain the ground truth distances for filtered KNN search,
  /// pre-conditioned on matching metadata predicates. Unlike standard neighbor distances
  /// which are computed from pure KNN, these incorporate predicate filtering so that
  /// recall does not degrade arbitrarily.
  ///
  /// @return An Optional containing the filtered neighbor distances, or empty if not available
  @Override
  public Optional<NeighborDistances> getFilteredNeighborDistances() {
    NeighborDistances cached = cachedFilteredNeighborDistances;
    if (cached != null) return Optional.of(cached);

    Optional<DSView> oView = getMatchingView(ViewKind.filtered_neighbors);
    if (oView.isEmpty()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    try {
      URL sourceUrl = resolveSourceUrl(dsView);
      Path contentPath = resolveContentPath(dsView);
      MAFileChannel channel = resolveMAFileChannel(contentPath, sourceUrl);
      DSWindow window = resolveWindow(dsView);
      String extension = extractExtension(sourceUrl);
      NeighborDistancesXvecImpl newview = new NeighborDistancesXvecImpl(
          channel, channel.size(), window, extension);
      cachedFilteredNeighborDistances = newview;
      viewResolutions.put(ViewKind.filtered_neighbors, new ViewResolution(channel, contentPath, window, extension));
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /// Returns the distance function used for this dataset.
  ///
  /// The distance function determines how similarity between vectors is calculated.
  /// This method retrieves the distance function from the dataset attributes if available,
  /// or defaults to COSINE if not specified.
  ///
  /// @return The distance function for this dataset
  @Override
  public DistanceFunction getDistanceFunction() {
    // Get the distance function from the dataset attributes if available
    if (datasetEntry.attributes() != null && datasetEntry.attributes()
        .containsKey("distance_function"))
    {
      String distanceFunctionStr = datasetEntry.attributes().get("distance_function");
      try {
        return DistanceFunction.valueOf(distanceFunctionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        // If the string doesn't match any enum value, fall back to default
      }
    }
    // Default to COSINE if not specified
    return DistanceFunction.COSINE;
  }

  /// Returns the license information for this dataset.
  ///
  /// This method retrieves the license information from the dataset attributes if available,
  /// or returns an empty string if not specified.
  ///
  /// @return The license information for this dataset
  @Override
  public String getLicense() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("license")) {
      return datasetEntry.attributes().get("license");
    }
    return "";
  }

  /// Returns the URL of this dataset.
  ///
  /// This is the base URL from which all dataset files are accessed.
  ///
  /// @return The URL of this dataset
  @Override
  public URL getUrl() {
    return datasetEntry.url();
  }

  /// Returns the model name for this dataset.
  ///
  /// This method retrieves the model name from the dataset attributes if available,
  /// or returns an empty string if not specified.
  ///
  /// @return The model name for this dataset
  @Override
  public String getModel() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("model")) {
      return datasetEntry.attributes().get("model");
    }
    return "";
  }

  /// Returns the vendor name for this dataset.
  ///
  /// This method retrieves the vendor name from the dataset attributes if available,
  /// or returns an empty string if not specified.
  ///
  /// @return The vendor name for this dataset
  @Override
  public String getVendor() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("vendor")) {
      return datasetEntry.attributes().get("vendor");
    }
    return "";
  }

  /// Looks up a token value by name.
  ///
  /// This method searches for a token in the following order:
  /// 1. In the dataset's tags map
  /// 2. In the SpecToken enum
  /// 3. Special handling for common tokens like "name", "url", and "license"
  ///
  /// @param tokenName 
  ///     The name of the token to look up
  /// @return An Optional containing the token value, or empty if not found
  @Override
  public Optional<String> lookupToken(String tokenName) {
    // First check if the token is in the dataset's tokens map
    if (datasetEntry.tags() != null && datasetEntry.tags().containsKey(tokenName)) {
      return Optional.ofNullable(datasetEntry.tags().get(tokenName));
    }

    // Then check if it's a standard token that we can derive from other properties
    try {
      // Try to find the token in the SpecToken enum
      Optional<SpecToken> tokenOpt = SpecToken.lookup(tokenName);
      if (tokenOpt.isPresent()) {
        // Use the token's apply method to get the value
        return tokenOpt.get().apply(this);
      }

      // Handle custom tokens not in the SpecToken enum
      if (tokenName.equalsIgnoreCase("name")) {
        return Optional.ofNullable(datasetEntry.name());
      } else if (tokenName.equalsIgnoreCase("url")) {
        return Optional.ofNullable(datasetEntry.url().toString());
      } else if (tokenName.equalsIgnoreCase("license")) {
        return Optional.ofNullable(getLicense());
      }

      return Optional.empty();
    } catch (IllegalArgumentException e) {
      // Not a standard token
      return Optional.empty();
    }
  }

  /// Replaces tokens in a template string with their values.
  ///
  /// This method uses the Templatizer to replace tokens in the format ${token_name}
  /// with their corresponding values from this dataset.
  ///
  /// @param template 
  ///     The template string containing tokens to replace
  /// @return An Optional containing the tokenized string, or empty if tokenization failed
  @Override
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
  }

  /// Returns the name of this dataset.
  ///
  /// @return The name of this dataset, or an empty string if not specified
  @Override
  public String getName() {
    return datasetEntry.name() != null ? datasetEntry.name() : "";
  }

  /// Returns a map of all available tokens for this dataset.
  ///
  /// This method collects tokens from multiple sources:
  /// 1. Standard metadata (name, url, model, vendor, license, distance_function)
  /// 2. Tokens from the SpecToken enum
  /// 3. Custom tokens from the dataset's tags
  ///
  /// @return A map of token names to token values
  @Override
  public Map<String, String> getTokens() {
    Map<String, String> tokens = new HashMap<>();

    // Add standard tokens
    tokens.put("name", getName());
    tokens.put("url", getUrl().toString());
    tokens.put("model", getModel());
    tokens.put("vendor", getVendor());
    tokens.put("license", getLicense());
    tokens.put("distance_function", getDistanceFunction().name());

    // Add tokens from SpecToken enum
    for (SpecToken token : SpecToken.values()) {
      token.apply(this).ifPresent(value -> tokens.put(token.name(), value));
    }

    // Add custom tokens from dataset tags
    if (datasetEntry.tags() != null) {
      tokens.putAll(datasetEntry.tags());
    }

    return tokens;
  }

  private URL resolveSourceUrl(DSView view) {
    if (view.getSource() == null || view.getSource().getPath() == null) {
      throw new IllegalArgumentException("No source path defined for view " + view.getName());
    }
    try {
      return new URL(datasetEntry.url(), view.getSource().getPath());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid source path '" + view.getSource().getPath() + "' for view " + view.getName(), e);
    }
  }

  private Path resolveContentPath(DSView view) {
    if (view.getSource() == null || view.getSource().getPath() == null) {
      throw new IllegalArgumentException("No source path defined for view " + view.getName());
    }
    Path datasetRoot = cachedir.resolve(datasetEntry.name());
    Path sharedPath = datasetRoot.resolve(view.getSource().getPath());
    Path legacyPath = datasetRoot.resolve(profile.getName()).resolve(view.getSource().getPath());
    if (existsEither(sharedPath)) {
      return sharedPath;
    }
    if (existsEither(legacyPath)) {
      return legacyPath;
    }
    return sharedPath;
  }

  private DSWindow resolveWindow(DSView view) {
    if (view == null) {
      return null;
    }
    DSWindow viewWindow = view.getWindow();
    if (viewWindow != null && !viewWindow.isEmpty()) {
      return viewWindow;
    }
    if (view.getSource() != null) {
      DSWindow sourceWindow = view.getSource().getWindow();
      if (sourceWindow != null && !sourceWindow.isEmpty()) {
        return sourceWindow;
      }
    }
    return viewWindow;
  }

  private boolean existsEither(Path contentPath) {
    if (Files.exists(contentPath)) {
      return true;
    }
    Path merklePath = contentPath.resolveSibling(contentPath.getFileName() + ".mrkl");
    return Files.exists(merklePath);
  }

  private String extractExtension(URL sourceUrl) {
    String file = sourceUrl.getFile();
    int dot = file.lastIndexOf('.');
    if (dot < 0 || dot == file.length() - 1) {
      throw new IllegalArgumentException("Unable to determine file extension for " + file);
    }
    return file.substring(dot + 1);
  }

  /// Promotes all cached views from MAFileChannel-backed reads to Path-based
  /// memory-mapped reads. Called after prebuffering completes so that subsequent
  /// reads use zero-copy mmap — the same fast path as {@link
  /// io.nosqlbench.vectordata.discovery.vector.FilesystemVectorTestDataView}.
  ///
  /// For each view that was created with an MAFileChannel, this method checks
  /// whether the channel reports the file as fully cached. If so, it creates
  /// a new Path-based XvecImpl backed by the local cache file and stores it
  /// as the cached view, replacing the channel-backed one.
  private void promoteToLocalViews() {
    for (Map.Entry<ViewKind, ViewResolution> entry : viewResolutions.entrySet()) {
      ViewKind kind = entry.getKey();
      ViewResolution res = entry.getValue();

      try {
        if (!res.channel.isFullyCached()) {
          continue;
        }
        Path cachePath = res.channel.getCacheFilePath();
        if (!Files.exists(cachePath)) {
          continue;
        }

        if (kind == ViewKind.base) {
          cachedBaseVectors = new BaseVectorsXvecImpl(cachePath, res.window, res.extension);
        } else if (kind == ViewKind.query) {
          cachedQueryVectors = new QueryVectorsXvecImpl(cachePath, res.window, res.extension);
        } else if (kind == ViewKind.indices) {
          cachedNeighborIndices = new NeighborIndicesXvecImpl(cachePath, res.window, res.extension);
        } else if (kind == ViewKind.neighbors) {
          cachedNeighborDistances = new NeighborDistancesXvecImpl(cachePath, res.window, res.extension);
        } else if (kind == ViewKind.filtered_indices) {
          cachedFilteredNeighborIndices = new NeighborIndicesXvecImpl(cachePath, res.window, res.extension);
        } else if (kind == ViewKind.filtered_neighbors) {
          cachedFilteredNeighborDistances = new NeighborDistancesXvecImpl(cachePath, res.window, res.extension);
        } else {
          logger.debug("No promotion handler for view kind: {}", kind);
        }
        logger.debug("Promoted {} view to Path-based mmap: {}", kind, cachePath);
      } catch (Exception e) {
        logger.warn("Failed to promote {} view to mmap, continuing with channel reads: {}", kind, e.getMessage());
      }
    }
  }

  /// Prebuffers all datasets in this test data view, then promotes all cached
  /// views from channel-based reads to Path-based memory-mapped reads for
  /// maximum read throughput.
  ///
  /// The returned future is a [ProgressIndicatingFuture] when any of the
  /// underlying dataset views provide progress tracking, aggregating total
  /// and current work across all datasets being prebuffered.
  ///
  /// @return A future that completes when all prebuffering and promotion is done
  @Override
  public CompletableFuture<Void> prebuffer() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Prebuffer base vectors if available
    getBaseVectors().ifPresent(baseVectors -> {
      futures.add(baseVectors.prebuffer());
    });

    // Prebuffer query vectors if available
    getQueryVectors().ifPresent(queryVectors -> {
      futures.add(queryVectors.prebuffer());
    });

    // Prebuffer neighbor indices if available
    getNeighborIndices().ifPresent(neighborIndices -> {
      futures.add(neighborIndices.prebuffer());
    });

    // Prebuffer neighbor distances if available
    getNeighborDistances().ifPresent(neighborDistances -> {
      futures.add(neighborDistances.prebuffer());
    });

    // Prebuffer filtered neighbor indices if available
    getFilteredNeighborIndices().ifPresent(filteredIndices -> {
      futures.add(filteredIndices.prebuffer());
    });

    // Prebuffer filtered neighbor distances if available
    getFilteredNeighborDistances().ifPresent(filteredDistances -> {
      futures.add(filteredDistances.prebuffer());
    });

    CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenRun(this::promoteToLocalViews);

    // Aggregate progress tracking from individual futures
    List<ProgressIndicator<?>> progressSources = new ArrayList<>();
    for (CompletableFuture<Void> f : futures) {
      if (f instanceof ProgressIndicator) {
        progressSources.add((ProgressIndicator<?>) f);
      }
    }
    if (progressSources.isEmpty()) {
      return allOf;
    }

    double bytesPerUnit = progressSources.get(0).getBytesPerUnit();
    return new ProgressIndicatingFuture<>(
        allOf,
        () -> progressSources.stream().mapToDouble(ProgressIndicator::getTotalWork).sum(),
        () -> progressSources.stream().mapToDouble(ProgressIndicator::getCurrentWork).sum(),
        bytesPerUnit
    );
  }
}
