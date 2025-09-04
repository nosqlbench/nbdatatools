package io.nosqlbench.vectordata.layout;

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


import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.vectordata.utils.SHARED;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/// Configuration data used to bring multiple sources into a single group data file
public class TestGroupLayout {
  private final FGroup profiles;
  private final FProfiles profile_defaults;
  private final RootGroupAttributes attributes;
  private final List<String> attachments;

  public TestGroupLayout(FGroup profiles, FProfiles profile_defaults, RootGroupAttributes attributes, List<String> attachments) {
    this.profiles = profiles;
    this.profile_defaults = profile_defaults;
    this.attributes = attributes;
    this.attachments = attachments;
  }

  public FGroup profiles() {
    return profiles;
  }

  public FProfiles profile_defaults() {
    return profile_defaults;
  }

  public RootGroupAttributes attributes() {
    return attributes;
  }

  public List<String> attachments() {
    return attachments;
  }

  /// Constant for the attributes section name in configuration files
  public static final String ATTRIBUTES = "attributes";
  /// Constant for the attachments section name in configuration files
  public static final String ATTACHMENTS = "attachments";

  /// Creates a TestGroupLayout from an HDF5 Group.
  ///
  /// This method extracts profile configurations, attributes, and attachments from an existing HDF5 group.
  ///
  /// @param group The HDF5 group to extract configuration from
  /// @return A new TestGroupLayout instance
  /// @throws RuntimeException If the group doesn't contain valid profile data
  public static TestGroupLayout fromGroup(Group group) {
    String profilesData = group.getAttribute("profiles").getData().toString();
    Map<String,?> profilesMap = null;
    if (profilesData.startsWith("{") && profilesData.endsWith("}")) {
      profilesMap = SHARED.mapFromJson(profilesData);
    } else {
      profilesMap = (Map<String,?>)SHARED.yamlLoader.loadFromString(profilesData);
    }
    FGroup profiles = FGroup.fromObject(profilesMap, null);

    List<String> attachments = new ArrayList<>();
    Node attachmentsNode = group.getChild(ATTACHMENTS);
    Group attachmentsGroup = (Group) attachmentsNode;
    if (attachmentsGroup == null) {
      attachmentsGroup.getChildren().values().stream()
          .filter(n -> n.getType() == io.jhdf.api.NodeType.DATASET).forEach(n -> {
            attachments.add(n.getName());
          });
    }
    return new TestGroupLayout(profiles, null, RootGroupAttributes.fromGroup(group), attachments);
  }

  /// Creates a TestGroupLayout from a YAML configuration string.
  ///
  /// This method parses a YAML string containing profile configurations, attributes, and attachments.
  ///
  /// @param yaml The YAML configuration string
  /// @return A new TestGroupLayout instance
  /// @throws RuntimeException If the YAML doesn't contain valid configuration data
  public static TestGroupLayout fromYaml(String yaml) {
    Object configObject = SHARED.yamlLoader.loadFromString(yaml);
    if (configObject instanceof Map<?, ?>) {
      Map<?, ?> m = (Map<?, ?>) configObject;

      FProfiles profileDefault = null;
      Object defaultsObject = m.get("profile_defaults");
      if (defaultsObject != null) {
        try {
          profileDefault = FProfiles.fromObject(defaultsObject, null);
        } catch (Exception e) {
          throw new RuntimeException("invalid profile_defaults format:\n" + yaml, e);
        }
      }

      List<String> addFilesList = Optional.ofNullable(m.get("attachments")).map(o -> {
        if (!(o instanceof List<?>)) {
          throw new RuntimeException("attachments must be a list of paths");
        }
        List<?> l = (List<?>) o;
        return l.stream().map(Object::toString).map(Path::of).map(String::valueOf).collect(Collectors.toList());
      }).orElse(List.of());

      Object profilesObject = m.get("profiles");
      if (profilesObject == null) {
        throw new RuntimeException("profiles is required");
      }

      RootGroupAttributes rga = null;
      if (m.get(ATTRIBUTES) != null) {
        rga = RootGroupAttributes.fromMap((Map<String, String>) m.get(ATTRIBUTES));
      }

      try {
        FGroup profiles = FGroup.fromObject(profilesObject, profileDefault);
        return new TestGroupLayout(
            profiles,
            profileDefault,
            rga,
            addFilesList
        );
      } catch (Exception e) {
        throw new RuntimeException("invalid profiles group config format:\n" + yaml, e);
      }
    } else {
      throw new RuntimeException("invalid facet group config format:" + yaml);
    }
  }

  /// Loads a TestGroupLayout from a file.
  ///
  /// This method reads a YAML configuration file and creates a TestGroupLayout from it.
  ///
  /// @param mappingFile The path to the YAML configuration file
  /// @return A new TestGroupLayout instance
  /// @throws RuntimeException If the file cannot be read or doesn't contain valid configuration data
  public static TestGroupLayout load(Path mappingFile) {
    String configdata = null;
    try {
      configdata = Files.readString(mappingFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return fromYaml(configdata);
  }

  /// Converts this TestGroupLayout to a YAML string.
  ///
  /// @return A YAML string representation of this TestGroupLayout
  public String toYaml() {
    Map<String, Object> data = toData();
    return SHARED.yamlDumper.dumpToString(data);
  }

  /// Converts this TestGroupLayout to a map structure suitable for serialization.
  ///
  /// @return A map containing the data from this TestGroupLayout
  /// @throws RuntimeException If profiles is null
  public Map<String, Object> toData() {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();

    if (attributes != null) {
      Map<String, String> attrs = new LinkedHashMap<>();
      attrs.put("model", attributes.model());
      attrs.put("url", attributes.url());
      attrs.put("distance_function", attributes.distance_function().name());
      attributes.notes().ifPresent(n -> attrs.put("notes", n));
      attrs.put("license", attributes.license());
      attrs.put("vendor", attributes.vendor());
      if (!attributes.tags().isEmpty()) {
        attrs.put("tags", SHARED.gson.toJson(attributes.tags()));
      }
      map.put(ATTRIBUTES, attrs);
    }

    if (profiles == null) {
      throw new RuntimeException("profiles is required");
    }

    map.put("profiles", profiles.toData());

    if (profile_defaults != null) {
      map.put("profile_defaults", profile_defaults.toData());
    }
    if (attachments != null && !attachments.isEmpty()) {
      map.put("attachments", attachments);
    }
    return map;
  }

  /// Creates a new TestGroupLayout with the specified attributes.
  ///
  /// @param attributes The new attributes to use
  /// @return A new TestGroupLayout with the specified attributes
  public TestGroupLayout withAttributes(RootGroupAttributes attributes) {
    return new TestGroupLayout(this.profiles, this.profile_defaults, attributes, this.attachments);
  }

  /// Creates a new TestGroupLayout with the specified profile defaults.
  ///
  /// @param profileDefaults The new profile defaults to use
  /// @return A new TestGroupLayout with the specified profile defaults
  public TestGroupLayout withProfileDefaults(FProfiles profileDefaults) {
    return new TestGroupLayout(this.profiles, profileDefaults, this.attributes, this.attachments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TestGroupLayout that = (TestGroupLayout) o;
    return Objects.equals(profiles, that.profiles) &&
           Objects.equals(profile_defaults, that.profile_defaults) &&
           Objects.equals(attributes, that.attributes) &&
           Objects.equals(attachments, that.attachments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(profiles, profile_defaults, attributes, attachments);
  }

  @Override
  public String toString() {
    return "TestGroupLayout{" +
      "profiles=" + profiles +
      ", profile_defaults=" + profile_defaults +
      ", attributes=" + attributes +
      ", attachments=" + attachments +
      '}';
  }
}
