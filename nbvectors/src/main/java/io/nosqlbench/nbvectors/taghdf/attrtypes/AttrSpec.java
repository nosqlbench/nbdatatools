package io.nosqlbench.nbvectors.taghdf.attrtypes;

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


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Attribute Specifier - a specific attribute within an HDF5 file,
/// with optional parent name.
/// ```
/// varname
/// :varname
/// .varname
/// /:varname
/// /.varname
/// /group1:varname
/// /group1.varname
/// /group1/group2:varname
/// /group1/group2.varname
///```
/// @param path the path component to the fully qualified attribute
/// @param attr the attribute name
public record AttrSpec(
    String path,
    String attr
)
{
  /// a pattern to match attr specs
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor"})
  public static final Pattern SPEC_PATTERN = Pattern.compile(
      """
          (?<path>/|(?:/[^:/.]+)+)?          # Optional HDF5 path (e.g., /, /group, /group1/group2)
          [:.]?                              # Optional : or . separating path from attribute
          (?<attr>[a-zA-Z_][a-zA-Z0-9_]*)    # Variable name (required, follows identifier rules)
          """, Pattern.COMMENTS
  );

  /// parse an attribute spec into an attribute spec
  /// @param spec The textual representation of an attribute
  /// @return an attribute spec
  public static AttrSpec parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid HDF5 attribute spec format: " + spec);
    }
    String path = m.group("path") != null ? m.group("path").trim() : "/";
    String attr = m.group("attr");

    return new AttrSpec(path, attr);
  }

  /// an attribute spec
  /// @param path the path component to the fully qualified attribute
  /// @param attr the attribute name
  public AttrSpec {
    // Example of a simple validation:
    if (attr == null || attr.isEmpty()) {
      throw new IllegalArgumentException("attr cannot be null or empty.");
    }
  }
}
