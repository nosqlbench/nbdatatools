package io.nosqlbench.vectordata;

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

import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.jhdf.exceptions.HdfInvalidPathException;
import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;
import io.nosqlbench.vectordata.internalapi.tokens.SpecDataSource;
import io.nosqlbench.vectordata.internalapi.tokens.Templatizer;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.layout.FSource;
import io.nosqlbench.vectordata.layout.FView;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.layout.FGroup;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import io.nosqlbench.vectordata.internalapi.datasets.TestDataKind;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.RootGroupAttributes;
import io.nosqlbench.vectordata.internalapi.tokens.SpecToken;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/// This is the entry to consuming Vector Test Data according to the documented spec.
/// This is the de-facto reference implementation of an accessor API that uses the documented
/// HDF5 data format. When there are any conflicts between this and the provided documentation,
/// then this implement should take precedence.
///
/// In a future edition, the documentation should be derived directly from this reference
/// implementation and the accompanying Javadoc.
public class TestDataGroup implements AutoCloseable {

  /// The default profile name used when no specific profile is requested
  public static final String DEFAULT_PROFILE = "default";
  /// The root HDF5 group containing all datasets
  private final Group group;
  /// The parsed profile configurations from the HDF5 file
  private final FGroup groupProfiles;
  /// The root group attributes parsed from the HDF5 file
  private RootGroupAttributes attributes;

  /// Cache of profile views to avoid recreating them
  private Map<String, TestDataView> profileCache = new LinkedHashMap<>();

  /// Name of the sources group in the HDF5 file
  public static final String SOURCES_GROUP = "sources";
  /// Name of the attachments group in the HDF5 file
  public static final String ATTACHMENTS_GROUP = "attachments";
  /// Name of the profiles attribute in the HDF5 file
  public static final String PROFILES_ATTR = "profiles";

  /// create a vector data reader
  /// @param path
  ///     the path to the HDF5 file
  public TestDataGroup(Path path) {
    this(new HdfFile(path));
  }

  /// create a vector data reader
  /// @param file
  ///     the HDF5 file
  public TestDataGroup(HdfFile file) {
    this((Group) file);

  }

  /// Creates a TestDataGroup from an HDF5 Group.
  ///
  /// @param group The HDF5 group to read data from
  public TestDataGroup(Group group) {
    this.group = group;
    this.attributes = RootGroupAttributes.fromGroup(group);
    this.groupProfiles = initConfig();
  }

  /// Initializes the profile configuration from the HDF5 file.
  ///
  /// @return The parsed profile configuration
  private FGroup initConfig() {
    TestGroupLayout testGroupLayout=null;
    Attribute profilesAttr = group.getAttribute(PROFILES_ATTR);
    if (profilesAttr == null) {
      return SyntheticDataConfig();
    }
    String profilesJson = profilesAttr.getData().toString();
    Map<String, ?> profilesData = SHARED.mapFromJson(profilesJson);
    FGroup fgroup = FGroup.fromObject(profilesData, null);
    return fgroup;
  }

  /// Creates a synthetic profile configuration when none is found in the HDF5 file.
  ///
  /// This method attempts to infer a configuration based on the available datasets.
  ///
  /// @return A synthetic profile configuration
  private FGroup SyntheticDataConfig() {
    //    Map<String,FProfile> fprofiles = new LinkedHashMap<>();
    Map<String, FView> fviews = new LinkedHashMap<>();
    //    List<FSource> sources = new ArrayList<>();
      group.getChildren().forEach((dsname, nodeValue) -> {
      if (nodeValue instanceof Dataset) {
        TestDataKind kind = TestDataKind.fromString(dsname);
        fviews.put(kind.name(), new FView(new FSource(dsname, FWindow.ALL), FWindow.ALL));
      }
      TestDataKind.fromOptionalString(dsname).ifPresent(s -> {

      });
      TestDataKind kind = TestDataKind.fromString(dsname);
    });
    FGroup fgroup = new FGroup(Map.of(DEFAULT_PROFILE, new FProfiles(fviews)));
    return fgroup;
  }

  /// Gets the name of this data group.
  ///
  /// @return The name of the data group
  public String getName() {
    return group.getName();
  }


  /// Gets a group node from the HDF5 file by path.
  ///
  /// @param path The path to the group
  /// @return An Optional containing the group node, or empty if not found or not a group
  private Optional<Node> getGroup(String path) {
    try {
      Node node = this.group.getByPath(path);
      if (node.isGroup()) {
        return Optional.of(node);
      } else {
        return Optional.empty();
      }
    } catch (HdfInvalidPathException e) {
      return Optional.empty();
    }
  }

  /// Gets any node from the HDF5 file by path.
  ///
  /// @param path The path to the node
  /// @return An Optional containing the node, or empty if not found
  private Optional<Node> getNode(String path) {
    try {
      Node node = this.group.getByPath(path);
      return Optional.of(node);
    } catch (HdfInvalidPathException e) {
      return Optional.empty();
    }
  }


  /// Looks up a token value across all profiles.
  ///
  /// @param tokenName The name of the token to look up
  /// @return A map of profile names to token values
  public Map<String, String> lookupTokens(String tokenName) {
    Map<String, String> tokens = new LinkedHashMap<>();
    for (String profile : getProfileNames()) {
      tokens.put(profile, getProfile(profile).lookupToken(tokenName).orElse(null));
    }
    return tokens;
  }


  /// Closes the underlying HDF5 file if this group is the root.
  ///
  /// @throws Exception If an error occurs while closing the file
  @Override
  public void close() throws Exception {
    if (group instanceof HdfFile) {
      ((HdfFile) group).close();
      return;
    }
  }

  /// Returns a string representation of this TestDataGroup.
  ///
  /// @return A string representation of this TestDataGroup
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("TestDataGroup{");
    sb.append("attributes=").append(attributes);
    sb.append(", group=").append(group);
    sb.append(", groupProfiles=").append(groupProfiles);
    sb.append(", profiles=").append(profileCache);
    sb.append('}');
    return sb.toString();
  }

  //  @Override
  //  public String toString() {
  //    StringBuilder sb = new StringBuilder();
  //    sb.append("VectorData()").append("\n").append(group.getFileAsPath()).append(") {\n");
  //    sb.append(" model: ").append(getModel()).append("\n");
  //    sb.append(" license: ").append(getLicense()).append("\n");
  //    sb.append(" vendor: ").append(getVendor()).append("\n");
  //    sb.append(" distance_function: ").append(getDistanceFunction()).append("\n");
  //    sb.append(" url: ").append(getUrl()).append("\n");
  //    getNotes().ifPresent(n -> sb.append(" notes: ").append(n).append("\n"));
  //    getBaseVectors().ifPresent(v -> sb.append(" base_vectors: ").append(v.toString()).append("\n"));
  //    getQueryVectors().ifPresent(v -> sb.append(" query_vectors: ").append(v.toString())
  //        .append("\n"));
  //    IntVectorConfigs indices = getNeighborIndices();
  //    if (!indices.isEmpty()) {
  //      sb.append(" neighbor_indices:\n");
  //      indices.forEach((k, v) -> {
  //        sb.append("  ").append(k).append(":").append(v).append("\n");
  //      });
  //    }
  //
  //    getNeighborDistances().ifPresent(v -> sb.append(" neighbor_distances: ").append(v.toString())
  //        .append("\n"));
  //    sb.append("}");
  //    return sb.toString();
  //  }

  /// Gets the first dataset found from a list of possible paths.
  ///
  /// @param names The paths to search for datasets
  /// @param <Dataset> the type of dataset to return
  /// @return An Optional containing the first dataset found, or empty if none found
  Optional<Dataset> getFirstDataset(String... names) {
    Dataset dataset = null;
    for (String name : names) {
      try {
        dataset = group.getDatasetByPath(name);
        return Optional.of(dataset);
      } catch (HdfInvalidPathException ignored) {
      }
    }
    return Optional.empty();
  }

  /// Gets the first dataset found from a list of possible paths, throwing an exception if none found.
  ///
  /// @param names The paths to search for datasets
  /// @return The first dataset found
  /// @throws HdfInvalidPathException If no dataset is found at any of the specified paths
  private Dataset getFirstDatasetRequired(String... names) {
    Optional<Dataset> firstDataset = getFirstDataset(names);
    return firstDataset.orElseThrow(() -> new HdfInvalidPathException(
        "none of the following datasets were found: " + String.join(",", names),
        this.group.getFileAsPath()
    ));
  }

  /// Get the full set of standard config tokens that are associated with this dataset.
  /// These are simple textual values to use for labeling results elsewhere
  /// @return the full set of standard config tokens
  public Map<String, String> getTokens(String profile) {
    Map<String, String> tokenMap = new LinkedHashMap<>();
    TestDataView tokenProfile = getProfile(profile);
    for (SpecToken specToken : SpecToken.values()) {
      specToken.apply(tokenProfile).ifPresent(t -> tokenMap.put(specToken.name(), t));
    }
    return tokenMap;
  }

  /// get the tags associated with this dataset
  /// @return the tags associated with this dataset
  public Map<String, String> getTags() {
    return RootGroupAttributes.fromGroup(group).tags();
  }

  /// Gets the set of configuration names available for a specific data kind.
  ///
  /// @param kind The kind of data to get configurations for
  /// @return A set of configuration names
  public Set<String> getConfigs(TestDataKind kind) {
    Set<String> configSet = new LinkedHashSet<>();
    try {
      Node child = this.group.getChild(kind.name());
      if (child instanceof Dataset) {
        configSet.add(DEFAULT_PROFILE);
      } else if (child instanceof Group g) {
        Map<String, Node> children = g.getChildren();
        configSet.addAll(children.keySet());
      }
    } catch (HdfInvalidPathException ignored) {
    }
    return configSet;
  }

  /// Gets a profile by name, returning an empty Optional if not found.
  ///
  /// @param profileName The name of the profile to get
  /// @return An Optional containing the profile, or empty if not found
  public Optional<TestDataView> getProfileOptionally(String profileName) {
    FProfiles fprofile = this.groupProfiles.profiles().get(profileName);
    if (fprofile == null) {
      return Optional.empty();
    }
    TestDataView profile =
        profileCache.computeIfAbsent(profileName, p -> new ProfileDataView(this, fprofile));
    return Optional.of(profile);
  }

  /// Gets the default profile.
  ///
  /// @return The default profile
  public TestDataView getDefaultProfile() {
    return this.getProfile(DEFAULT_PROFILE);
  }
  /// Gets a profile by name, throwing an exception if not found.
  ///
  /// @param profileName The name of the profile to get
  /// @return The profile
  /// @throws IllegalArgumentException If the profile is not found
  public TestDataView getProfile(String profileName) {
    return getProfileOptionally(profileName).orElseThrow(() -> new IllegalArgumentException(
        "profile '" + profileName + "' not found in " + this));
  }

  /// Gets the set of all profile names available in this data group.
  ///
  /// @return A set of profile names
  public Set<String> getProfileNames() {
    Set<String> names = this.groupProfiles.profiles().keySet();
    names.forEach(n -> profileCache.computeIfAbsent(n, p -> null));
    return profileCache.keySet();
  }

  /// Gets an attribute from the root group by name.
  ///
  /// @param attrname The name of the attribute to get
  /// @return The attribute, or null if not found
  public Attribute getAttribute(String attrname) {
    Attribute attribute = group.getAttribute(attrname);
    return attribute;
  }

  /// Tokenizes a template string using the tokens available in this data group.
  ///
  /// @param template The template string to tokenize
  /// @return An Optional containing the tokenized string, or empty if tokenization failed
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
  }

  /// Gets the cache of all profiles, initializing it if empty.
  ///
  /// @return A map of profile names to profile views
  public synchronized Map<String, TestDataView> getProfileCache() {
    if (profileCache.isEmpty()) {
      profileCache.putAll(getProfileNames().stream()
          .collect(Collectors.toMap(p -> p, p -> getProfile(p))));
    }
    return profileCache;
  }

  /// Gets all available tokens for this data group across all profiles.
  ///
  /// @return A map of token names to token values
  public Map<String,String> getTokens() {
      Map<String,String> tokens = new LinkedHashMap<>();
      Set<String> tokenNames = new LinkedHashSet<>();
      getProfileCache().values().forEach(p -> tokenNames.addAll(p.getTokens().keySet()));
      tokenNames.forEach(t -> tokens.put(t,lookupToken(t).orElse(null)));
      return tokens;
  }

  /// Looks up a token value, first in the root attributes, then across all profiles.
  ///
  /// @param tokenName The name of the token to look up
  /// @return An Optional containing the token value, or empty if not found
  private Optional<String> lookupToken(String tokenName) {

    Attribute rootAttr = group.getAttribute(tokenName);
    if (rootAttr != null) {
      return Optional.of(rootAttr.getData().toString());
    }

    Set<String> values = getProfileCache().values().stream().map(p -> p.lookupToken(tokenName))
        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());
    if (values.isEmpty()) {
      return Optional.empty();
    }
    if (values.size() == 1) {
      return Optional.of(values.iterator().next());
    }
    boolean numeric = true;
    boolean textual = true;
    for (String value : values) {
      if (value.matches("[0-9]+")) {
        textual = false;
      } else {
        numeric = false;
      }
      if (!numeric && !textual) {
        return Optional.of("_");
      }
      if (numeric) {
        long min = values.stream().map(Long::parseLong).min(Long::compareTo).orElseThrow();
        long max = values.stream().map(Long::parseLong).min(Long::compareTo).orElseThrow();
        return Optional.of(min + "to" + max);
      } else {
        return Optional.of(String.valueOf(values.size() + "V"));
      }
    }
    return Optional.empty();
  }

  /// Gets the distance function used for this dataset.
  ///
  /// @return The distance function, defaulting to COSINE if not specified
  public DistanceFunction getDistanceFunction() {
    return Optional.ofNullable(group.getAttribute(SpecToken.distance_function.name())).map(Attribute::getData)
        .map(String::valueOf).map(String::toUpperCase)
        .map(DistanceFunction::valueOf).orElse(DistanceFunction.COSINE);
  }

  //  public String toJson() {
//    return SHARED.gson.toJson(Map.of(
////        "attributes", attributes
//        "profiles", groupProfiles
//    ));
//  }
}
