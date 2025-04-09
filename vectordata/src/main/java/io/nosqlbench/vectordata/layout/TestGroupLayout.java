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
import io.nosqlbench.vectordata.SHARED;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.RootGroupAttributes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Configuration data used to bring multiple sources into a single group data file
/// @param profiles
///     the profiles for this group
/// @param profile_defaults
///     the defaults to be added to profiles during import processing. This
///                     is an ephemeral setting which is not persisted into data files
/// @param attributes
///     the attributes for the root group of this group data file
/// @param attachments
///     the additional files to add to this group. These are brought in as datasets with a single
///                  string value, under the dataset name of the file name, under the group
///         "attachments"
public record TestGroupLayout(
    FGroup profiles,
    FProfiles profile_defaults,
    RootGroupAttributes attributes,
    List<String> attachments
)
{

  public static final String ATTRIBUTES = "attributes";
  public static final String ATTACHMENTS = "attachments";

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

  public static TestGroupLayout fromYaml(String yaml) {
    Object configObject = SHARED.yamlLoader.loadFromString(yaml);
    if (configObject instanceof Map<?, ?> m) {

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
        if (!(o instanceof List<?> l)) {
          throw new RuntimeException("attachments must be a list of paths");
        }
        return l.stream().map(Object::toString).map(Path::of).map(String::valueOf).toList();
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

  public static TestGroupLayout load(Path mappingFile) {
    String configdata = null;
    try {
      configdata = Files.readString(mappingFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return fromYaml(configdata);
  }

  public String toYaml() {
    Map<String, Object> data = toData();
    return SHARED.yamlDumper.dumpToString(data);
  }

  private Map<String, Object> toData() {
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

  public TestGroupLayout withAttributes(RootGroupAttributes attributes) {
    return new TestGroupLayout(this.profiles, this.profile_defaults, attributes, this.attachments);
  }

  public TestGroupLayout withProfileDefaults(FProfiles profileDefaults) {
    return new TestGroupLayout(this.profiles, profileDefaults, this.attributes, this.attachments);
  }
}
