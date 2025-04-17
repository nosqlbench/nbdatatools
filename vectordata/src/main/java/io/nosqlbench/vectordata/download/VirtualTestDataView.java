package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.download.merkle.MerkleRAF;
import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;
import io.nosqlbench.vectordata.internalapi.datasets.api.BaseVectors;
import io.nosqlbench.vectordata.internalapi.datasets.api.NeighborDistances;
import io.nosqlbench.vectordata.internalapi.datasets.api.NeighborIndices;
import io.nosqlbench.vectordata.internalapi.datasets.api.QueryVectors;
import io.nosqlbench.vectordata.internalapi.datasets.api.TestDataKind;
import io.nosqlbench.vectordata.internalapi.datasets.impl.xvec.BaseVectorsXvecImpl;
import io.nosqlbench.vectordata.internalapi.datasets.impl.xvec.FloatVectorsXvecImpl;
import io.nosqlbench.vectordata.internalapi.tokens.SpecToken;
import io.nosqlbench.vectordata.internalapi.tokens.Templatizer;
import io.nosqlbench.vectordata.layout.manifest.DSProfile;
import io.nosqlbench.vectordata.layout.manifest.DSView;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class VirtualTestDataView implements TestDataView {

  private final DatasetEntry datasetEntry;
  private final DSProfile profile;
  private Path cachedir = Path.of(System.getProperty("user.home"), ".cache", "jvector");

  public VirtualTestDataView(Path cachedir, DatasetEntry datasetEntry, DSProfile profile) {
    this.datasetEntry = datasetEntry;
    this.profile = profile;
    this.cachedir = cachedir;
  }

  @Override
  public Optional<BaseVectors> getBaseVectors() {
    Optional<DSView> oView = getMatchingView("base_vectors");
    if (!oView.isPresent()) {
      return Optional.empty();
    }
    DSView dsView = oView.get();
    String sourcePath = dsView.getSource().getPath();
    try {
      URL sourceContentURL = new URL(datasetEntry.url(), sourcePath);
      Path contentPath =
          cachedir.resolve(datasetEntry.name()).resolve(profile.getName()).resolve(sourcePath);
      MerkleRAF merkleRAF = new MerkleRAF(contentPath, sourceContentURL.toString());
      String extension =
          sourceContentURL.getFile().substring(sourceContentURL.getFile().lastIndexOf('.') + 1);
      BaseVectorsXvecImpl newview =
          new BaseVectorsXvecImpl(merkleRAF, merkleRAF.length(), dsView.getWindow(), extension);
      return Optional.of(newview);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<DSView> getMatchingView(String viewkind) {
    TestDataKind testDataKind = TestDataKind.fromString(viewkind);
    Set<String> allNames = testDataKind.getAllNames();

    for (String viewName : profile.keySet()) {
      if (allNames.contains(viewName)) {
        return Optional.of(profile.get(viewName));
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<QueryVectors> getQueryVectors() {
    DSView view = profile.get("query_vectors");
    if (view == null) {
      return Optional.empty();
    }
    try {
      URL sourceURL = new URL(datasetEntry.url(), view.getSource().getPath());
      MerkleRAF merkleRAF = new MerkleRAF(null, sourceURL.toString());
      FloatVectorsXvecImpl newview = new FloatVectorsXvecImpl(
          merkleRAF,
          merkleRAF.length(),
          view.getWindow(),
          sourceURL.getFile()
      );
      // Cast is needed because FloatVectorsXvecImpl implements QueryVectors
      return Optional.of((QueryVectors) newview);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<NeighborIndices> getNeighborIndices() {
    DSView view = profile.get("neighbor_indices");
    if (view == null) {
      return Optional.empty();
    }
    try {
      URL sourceURL = new URL(datasetEntry.url(), view.getSource().getPath());
      MerkleRAF merkleRAF = new MerkleRAF(null, sourceURL.toString());
      // Assuming there's an implementation for NeighborIndices similar to BaseVectorsXvecImpl
      // This would need to be implemented or adapted from existing code
      // For now, returning empty to avoid compilation errors
      return Optional.empty();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<NeighborDistances> getNeighborDistances() {
    DSView view = profile.get("neighbor_distances");
    if (view == null) {
      return Optional.empty();
    }
    try {
      URL sourceURL = new URL(datasetEntry.url(), view.getSource().getPath());
      MerkleRAF merkleRAF = new MerkleRAF(null, sourceURL.toString());
      // Assuming there's an implementation for NeighborDistances similar to BaseVectorsXvecImpl
      // This would need to be implemented or adapted from existing code
      // For now, returning empty to avoid compilation errors
      return Optional.empty();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

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

  @Override
  public String getLicense() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("license")) {
      return datasetEntry.attributes().get("license");
    }
    return "";
  }

  @Override
  public URL getUrl() {
    return datasetEntry.url();
  }

  @Override
  public String getModel() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("model")) {
      return datasetEntry.attributes().get("model");
    }
    return "";
  }

  @Override
  public String getVendor() {
    if (datasetEntry.attributes() != null && datasetEntry.attributes().containsKey("vendor")) {
      return datasetEntry.attributes().get("vendor");
    }
    return "";
  }

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

  @Override
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
  }

  @Override
  public String getName() {
    return datasetEntry.name() != null ? datasetEntry.name() : "";
  }

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
}
