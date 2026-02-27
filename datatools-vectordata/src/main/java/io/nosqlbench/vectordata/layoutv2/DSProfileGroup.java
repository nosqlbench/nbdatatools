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


import io.nosqlbench.vectordata.utils.SHARED;

import java.util.LinkedHashMap;
import java.util.Map;

/// Represents a group of profiles, each identified by a name.
/// Extends LinkedHashMap to store profiles by name.
public class DSProfileGroup extends LinkedHashMap<String, DSProfile> {

  /// Constructs a new instance of DSProfileGroup.
  public DSProfileGroup() {
  }

  /// Adds a new profile with the specified name to this group.
  /// @param profile1 The name of the profile to add
  /// @return The newly created profile
  public DSProfile addProfile(String profile1) {
    DSProfile profile = new DSProfile();
    this.put(profile1, profile);
    return profile;
  }

  /// Adds a profile to this group and returns this group for method chaining.
  /// @param name The name of the profile
  /// @param profile The profile to add
  /// @return This profile group for method chaining
  public DSProfileGroup addProfile(String name, DSProfile profile) {
    this.put(name, profile);
    return this;
  }

  /// Creates a DSProfileGroup from a map of data.
  ///
  /// When a "default" profile is present, it is parsed first and used as the base
  /// for all other profiles (they inherit its views). This mirrors the two-pass
  /// inheritance logic in FGroup.fromObject().
  ///
  /// @param profiles The map of data to create the profile group from
  /// @return A new DSProfileGroup instance
  public static DSProfileGroup fromData(Object profiles) {
    Map<String,?> profilesMap = null;
    if (profiles instanceof Map<?, ?>) {
      profilesMap = (Map<String,?>) profiles;
    } else if (profiles instanceof String) {
      profilesMap = SHARED.mapFromJson((String) profiles);
    } else {
      throw new RuntimeException("invalid profiles format:" + profiles);
    }

    DSProfileGroup profileGroup = new DSProfileGroup();

    // First pass: parse "default" profile to use as base for inheritance
    DSProfile defaultProfile = null;
    Object defaultObj = profilesMap.get("default");
    if (defaultObj != null) {
      defaultProfile = DSProfile.fromData(defaultObj);
      defaultProfile.setName("default");
      profileGroup.addProfile("default", defaultProfile);
    }

    // Second pass: parse all other profiles, inheriting from default
    final DSProfile finalDefault = defaultProfile;
    profilesMap.forEach((k, v) -> {
      if ("default".equals(k)) {
        return; // already processed
      }
      DSProfile profile = DSProfile.fromData(v);
      // Inherit views from default profile that are not overridden
      if (finalDefault != null) {
        finalDefault.forEach((viewName, viewDef) -> profile.putIfAbsent(viewName, viewDef));
        // Inherit maxk if not specified
        if (profile.getMaxk() == null && finalDefault.getMaxk() != null) {
          profile.setMaxk(finalDefault.getMaxk());
        }
      }
      profile.setName(k);
      profileGroup.addProfile(k, profile);
    });
    return profileGroup;
  }


}
