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


import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.utils.SHARED;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/// Represents a group of profiles for faceted data access.
///
/// A group contains multiple profiles, each with a name and a set of views.
/// This allows for different ways of accessing and processing the same data.
public class FGroup {
  private final Map<String, FProfiles> profiles;
  private final static Type type = new TypeToken<>(){}.getType();

  public FGroup(Map<String, FProfiles> profiles) {
    this.profiles = profiles;
  }

  public Map<String, FProfiles> profiles() {
    return profiles;
  }
  /// Creates an FGroup from a JSON string.
  ///
  /// @param json The JSON string to parse
  /// @return The created FGroup
  public static FGroup fromJSON(String json) {

    Map<String, FProfiles> facets = SHARED.gson.fromJson(json, type);
    return new FGroup(facets);
  }

  /// Creates an FGroup from an object representation.
  ///
  /// @param facetsObject The object to convert to an FGroup
  /// @param defaultProfile The default profile to use if none is specified
  /// @return The created FGroup
  public static FGroup fromObject(Object facetsObject, FProfiles defaultProfile) {
    if (facetsObject instanceof Map<?,?>) {
      Map<?,?> m = (Map<?,?>) facetsObject;
      Map<String, FProfiles> facets = new LinkedHashMap<>();

      // First pass: Look for "default" profile to use as the base for other profiles
      FProfiles effectiveDefault = defaultProfile;
      Object defaultProfileObject = m.get("default");
      if (defaultProfileObject != null) {
        try {
          // Parse default profile (with no inheritance, or inherit from profile_defaults if provided)
          effectiveDefault = FProfiles.fromObject(defaultProfileObject, defaultProfile);
        } catch (Exception e) {
          throw new RuntimeException("invalid 'default' profile format:" + defaultProfileObject, e);
        }
      }

      // Second pass: Load all profiles with inheritance from effective default
      final FProfiles finalDefault = effectiveDefault;
      m.forEach((k,v) -> {
        String profileName = k.toString();
        try {
          // If this is "default", use already parsed version; otherwise inherit from default
          FProfiles fprofile;
          if ("default".equals(profileName)) {
            fprofile = finalDefault;
          } else {
            fprofile = FProfiles.fromObject(v, finalDefault);
          }
          facets.put(profileName, fprofile);
        } catch (Exception e) {
          throw new RuntimeException("invalid profile format for key[" + k + "]:" + v, e);
        }
      });
      return new FGroup(facets);
    }
    return null;
  }

  /// Converts this group to a JSON string.
  ///
  /// @return The JSON representation of this group
  public String toJson() {
    return SHARED.gson.toJson(this);
  }

  /// Converts this group to a data object.
  ///
  /// @return The data object representation of this group
  public Object toData() {
    Map<String,Object> map = new LinkedHashMap<>();
    profiles.forEach((k,v) -> {
      map.put(k,v.toData());
    });
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FGroup fGroup = (FGroup) o;
    return Objects.equals(profiles, fGroup.profiles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(profiles);
  }

  @Override
  public String toString() {
    return "FGroup{" +
      "profiles=" + profiles +
      '}';
  }

}
