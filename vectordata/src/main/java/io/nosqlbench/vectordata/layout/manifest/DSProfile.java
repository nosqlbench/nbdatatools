package io.nosqlbench.vectordata.layout.manifest;

import java.util.LinkedHashMap;
import java.util.Map;

/// Represents a profile that contains a collection of named views.
/// Extends LinkedHashMap to store views by name.
public class DSProfile extends LinkedHashMap<String, DSView> {

  /// Creates an empty profile with no views.
  public DSProfile() {
  }
  /// Creates a profile with the specified map of views.
  /// @param views The map of views to include in this profile
  public DSProfile(Map<String, DSView> views) {
    super(views);
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
    if (views instanceof Map<?,?> m) {
      vmap = m;
    } else {
      throw new RuntimeException("invalid profile format:" + views);
    }
    vmap.forEach((k, v) -> {
      viewMap.put(k.toString(),DSView.fromData(v));
    });
    return new DSProfile(viewMap);
  }

}
