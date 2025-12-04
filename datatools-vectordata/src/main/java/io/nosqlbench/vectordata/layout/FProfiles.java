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


import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.util.LinkedHashMap;
import java.util.Map;

/// Represents a collection of views for a profile.
///
/// A profile contains multiple views, each with a name and a set of parameters.
/// This allows for different ways of accessing and processing the same data.
public class FProfiles {
  /// A map of view names to view definitions
  private final Map<String, FView> views;
  /// The maximum neighborhood size (k) for this profile, if specified
  private final Integer maxk;

  public FProfiles(Map<String, FView> views, Integer maxk) {
    this.views = views;
    this.maxk = maxk;
  }

  public FProfiles(Map<String, FView> views) {
    this(views, null);
  }

  public Map<String, FView> views() {
    return views;
  }

  /// Get the maximum neighborhood size for this profile
  /// @return the maxk value, or null if not specified
  public Integer maxk() {
    return maxk;
  }

  /// Creates an FProfiles from an object representation.
  ///
  /// @param v The object to convert to an FProfiles
  /// @param defaultProfile The default profile to use if none is specified
  /// @return The created FProfiles
  public static FProfiles fromObject(Object v, FProfiles defaultProfile) {
    if (v instanceof FProfiles) {
      FProfiles fp = (FProfiles) v;
      return fp;
    } else if (v instanceof Map<?, ?>) {
      Map<?, ?> pmap = (Map<?, ?>) v;

      Object viewsObject = pmap.get("views");
      if (viewsObject instanceof Map<?, ?>) {
        return fromObject(viewsObject, defaultProfile);
      }
      Map<String, FView> views = new LinkedHashMap<>();
      if (defaultProfile != null) {
        views.putAll(defaultProfile.views());
      }

      // Extract maxk if present, otherwise inherit from default profile
      Integer maxk = null;
      Object maxkObj = pmap.get("maxk");
      if (maxkObj instanceof Number) {
        maxk = ((Number) maxkObj).intValue();
      } else if (maxkObj instanceof String) {
        try {
          maxk = Integer.parseInt((String) maxkObj);
        } catch (NumberFormatException e) {
          throw new RuntimeException("invalid maxk value: " + maxkObj, e);
        }
      } else if (defaultProfile != null && defaultProfile.maxk() != null) {
        // Inherit maxk from default profile if not specified
        maxk = defaultProfile.maxk();
      }

      pmap.forEach((pk, pv) -> {
        String kindSpec = pk.toString();
        // Skip metadata fields
        if ("maxk".equals(kindSpec)) {
          return;
        }
        try {
          TestDataKind kind = TestDataKind.fromString(kindSpec); // convert to canonical
          FView fview = FView.fromObject(pv);
          views.put(kind.name(), fview); // use canonical name for consistent lookup
        } catch (Exception e) {
          throw new RuntimeException("invalid profile format for FView key[" + pk + "]:" + v, e);
        }
      });
      return new FProfiles(views, maxk);
    } else {
      throw new RuntimeException("invalid profile format:" + v);
    }
  }

  /// Converts this profile to a data object.
  ///
  /// @return The data object representation of this profile
  public Map<String,Object> toData() {
    Map<String,Object> map = new LinkedHashMap<>();
    if (maxk != null) {
      map.put("maxk", maxk);
    }
    views.forEach((k,v) -> {
      map.put(k,v.toData());
    });
    return map;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FProfiles that = (FProfiles) obj;
    return java.util.Objects.equals(views, that.views) &&
           java.util.Objects.equals(maxk, that.maxk);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(views, maxk);
  }

  @Override
  public String toString() {
    return "FProfiles{views=" + views + ", maxk=" + maxk + '}';
  }
}
