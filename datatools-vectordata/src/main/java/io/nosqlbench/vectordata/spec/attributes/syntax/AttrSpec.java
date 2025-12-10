package io.nosqlbench.vectordata.spec.attributes.syntax;

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

/// Attribute specifier - identifies an attribute target within a dataset structure,
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
public class AttrSpec {
    /// the path component to the fully qualified attribute
    private final String path;
    /// the attribute name
    private final String attr;
    
    public AttrSpec(String path, String attr) {
        // Example of a simple validation:
        if (attr == null || attr.isEmpty()) {
            throw new IllegalArgumentException("attr cannot be null or empty.");
        }
        this.path = path;
        this.attr = attr;
    }
    
    /// @return the path component to the fully qualified attribute
    public String path() {
        return path;
    }
    
    /// @return the attribute name
    public String attr() {
        return attr;
    }
  /// a pattern to match attr specs
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor"})
  public static final Pattern SPEC_PATTERN = Pattern.compile(
      "(?<path>/|(?:/[^:/.]+)+)?          # Optional path (e.g., /, /group, /group1/group2)\n" +
          "[:.]?                              # Optional : or . separating path from attribute\n" +
          "(?<attr>[a-zA-Z_][a-zA-Z0-9_]*)    # Variable name (required, follows identifier rules)\n", Pattern.COMMENTS
  );

  /// parse an attribute spec into an attribute spec
  /// @param spec The textual representation of an attribute
  /// @return an attribute spec
  public static AttrSpec parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid attribute spec format: " + spec);
    }
    String path = m.group("path") != null ? m.group("path").trim() : "/";
    String attr = m.group("attr");

    return new AttrSpec(path, attr);
  }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AttrSpec attrSpec = (AttrSpec) obj;
        return path.equals(attrSpec.path) &&
               attr.equals(attrSpec.attr);
    }

    @Override
    public int hashCode() {
        int result = path.hashCode();
        result = 31 * result + attr.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AttrSpec{path='" + path + "', attr='" + attr + "'}";
    }

}
