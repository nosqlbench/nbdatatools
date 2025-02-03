package io.nosqlbench.nbvectors.taghdf.attrtypes;

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
public record AttrSpec(
    /// The path to the parent node of the attribute.
    String path,
    /// The name of the attribute.
    String attr
)
{
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor"})
  public static final Pattern SPEC_PATTERN = Pattern.compile(
      """
          (?<path>/|(?:/[^:/.]+)+)? # Optional HDF5 path (e.g., /, /group, /group1/group2)
          [:.]?                              # Optional : or . separating path from attribute
          (?<attr>[a-zA-Z_][a-zA-Z0-9_]*) # Variable name (required, follows identifier rules)
          """, Pattern.COMMENTS
  );

  /// @throws IllegalArgumentException
  ///     if the string does not match the expected format.
  public static AttrSpec parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid HDF5 attribute spec format: " + spec);
    }
    String path = m.group("path") != null ? m.group("path").trim() : "/";
    String attr = m.group("attr");

    return new AttrSpec(path, attr);
  }

  public AttrSpec {
    // Example of a simple validation:
    if (attr == null || attr.isEmpty()) {
      throw new IllegalArgumentException("attr cannot be null or empty.");
    }
  }
}
