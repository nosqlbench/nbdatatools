package io.nosqlbench.vectordata.layoutv2;

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


import java.util.Map;
import java.util.Objects;

/// Root class for dataset configuration.
/// Contains a name and a group of profiles that define how to access the data.
public class DSRoot {
  /// The name of this dataset configuration
  private String name;
  /// The group of profiles that define how to access the data
  private DSProfileGroup profiles = new DSProfileGroup();

  /// Creates a new dataset configuration with the specified name and profile group.
  /// @param name The name of this dataset configuration
  /// @param dsProfiles The group of profiles that define how to access the data
  public DSRoot(String name, DSProfileGroup dsProfiles) {
    this.name = name;
    this.profiles = dsProfiles;
  }

  /// Creates a new dataset configuration with the specified name and an empty profile group.
  /// @param name The name of this dataset configuration
  public DSRoot(String name) {
    this.name = name;
    this.profiles = new DSProfileGroup();
  }

  /// Creates a new dataset configuration with a default name ("unnamed") and an empty profile group.
  public DSRoot() {
    this.name = "unnamed";
    this.profiles = new DSProfileGroup();
  }


  /// Returns a string representation of this dataset configuration.
  /// @return A string representation of this dataset configuration
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("DSRoot{");
    sb.append("name='").append(name).append('\'');
    sb.append(", profiles=").append(profiles);
    sb.append('}');
    return sb.toString();
  }

  /// Adds a profile with the specified name to this dataset configuration.
  /// @param name The name of the profile
  /// @param profile The profile to add
  public void addProfile(String name, DSProfile profile) {
    this.profiles.put(name, profile);
  }
  /// Gets the group of profiles that define how to access the data.
  /// @return The profile group
  public DSProfileGroup getProfiles() {
    return this.profiles;
  }
  /// Gets the name of this dataset configuration.
  /// @return The name
  public String getName() {
    return this.name;
  }
  /// Sets the name of this dataset configuration.
  /// @param name The name
  public void setName(String name) {
    this.name = name;
  }
  /// Sets the group of profiles that define how to access the data.
  /// @param profiles The profile group
  public void setProfiles(DSProfileGroup profiles) {
    this.profiles = profiles;
  }

  /// Creates and adds a new profile with the specified name to this dataset configuration.
  /// @param profile1 The name of the profile
  /// @return The newly created profile
  public DSProfile addProfile(String profile1) {
    DSProfile profile = new DSProfile();
    this.addProfile(profile1, profile);
    return profile;
  }

  /// Creates a DSRoot from a map of data.
  /// @param data The map of data to create the root from
  /// @return A new DSRoot instance
  public DSRoot fromData(Map<String, Object> data) {
    String name = data.containsKey("name") ? data.get("name").toString() : null;
    DSProfileGroup profiles = DSProfileGroup.fromData((Map<String, Object>) data.get("profiles"));
    return new DSRoot(name, profiles);
  }
  /// Compares this dataset configuration with another object for equality.
  /// @param o The object to compare with
  /// @return True if the objects are equal, false otherwise
  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof DSRoot))
      return false;

    DSRoot dsRoot = (DSRoot) o;
    return Objects.equals(name, dsRoot.name) && Objects.equals(profiles, dsRoot.profiles);
  }

  /// Returns a hash code for this dataset configuration.
  /// @return A hash code value for this dataset configuration
  @Override
  public int hashCode() {
    int result = Objects.hashCode(name);
    result = 31 * result + Objects.hashCode(profiles);
    return result;
  }
}
