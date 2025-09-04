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


import java.util.LinkedHashMap;
import java.util.Map;

/// Represents a profile that contains a collection of named views.
/// Extends LinkedHashMap to store views by name.
public class DSProfile extends LinkedHashMap<String, DSView> {

  /// The name of the profile
  private String name;

  /// Creates an empty profile with no views.
  public DSProfile() {
  }
  /// Creates a profile with the specified map of views.
  /// @param views The map of views to include in this profile
  public DSProfile(Map<String, DSView> views) {
    super(views);
  }

  /// Create a new DSProfile
  /// @param name The profile name
  /// @param views The map of views to include in this profile
  public DSProfile(String name, Map<String, DSView> views) {
    super(views);
    this.name = name;
  }

  /// Adds a new view with the specified name to this profile.
  /// @param view1 The name of the view to add
  /// @return The newly created view
  public DSView addView(String view1) {
    DSView view = new DSView(view1);
    this.put(view1, view);
    return view;
  }

  /// Adds a view to this profile and returns this profile for method chaining.
  /// @param name The name of the view
  /// @param view The view to add
  /// @return This profile for method chaining
  public DSProfile addView(String name, DSView view) {
    this.put(name, view);
    return this;
  }

  /// Creates a DSProfile from a map of data.
  /// @param views The map of data to create the profile from
  /// @return A new DSProfile instance
  public static DSProfile fromData(Object views) {
    Map<?,?> vmap = null;
    Map<String, DSView> viewMap = new LinkedHashMap<>();
    if (views instanceof Map<?,?>) {
      vmap = (Map<?,?>) views;
    } else {
      throw new RuntimeException("invalid profile format:" + views);
    }
    vmap.forEach((k, v) -> {
      viewMap.put(k.toString(),DSView.fromData(v));
    });
    return new DSProfile(viewMap);
  }

  /// Get the name of the profile
  /// @return The name of the profile
  public String getName() {
    return this.name;
  }

  /// Set the name of the dataset profile
  /// @return This DSProfile for method chaining
  /// @param name The name of the dataset profile
  public DSProfile setName(String name) {
    this.name = name;
    return this;
  }
}
