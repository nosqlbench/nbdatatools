package io.nosqlbench.nbvectors.taghdf.attrtypes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// This record captures a basic string specification for assigning an attribute to an hdf5 parent.
/// The format can support any of these variants:
/// ```
/// varname=(String)value
/// :varname=value
/// .varname=value
/// /:varname=value
/// /.varname=value
/// /group1:varname=(int)value
/// /group1.varname=value
/// /group1/group2:varname=value
/// /group1/group2.varname=value
///```
public record AttrSet(
    AttrSpec attrname, AttrValue<?> attrvalue
)
{
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor", "EscapedSpace"})
  private static final Pattern SPEC_PATTERN = Pattern.compile(
      "(?<attrname>" + AttrSpec.SPEC_PATTERN.pattern() + ")\s*=\s*(?<attrvalue>"
      + AttrValue.SPEC_PATTERN.pattern() + ")", Pattern.COMMENTS
  );

  public static AttrSet parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid HDF5 attribute spec format: " + spec);
    }
    AttrSpec attrname = AttrSpec.parse(m.group("attrname"));
    AttrValue<?> attrvalue = AttrValue.parse(m.group("attrvalue"));

    return new AttrSet(attrname, attrvalue);
  }

}
