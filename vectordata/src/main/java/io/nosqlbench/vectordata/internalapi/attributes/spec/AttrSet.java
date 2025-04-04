package io.nosqlbench.vectordata.internalapi.attributes.spec;

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
/// @param attrname the attribute spec for the attribute to modify
/// @param attrvalue the attribute value to set
public record AttrSet(
    AttrSpec attrname, AttrValue<?> attrvalue
)
{
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor", "EscapedSpace"})
  private static final Pattern SPEC_PATTERN = Pattern.compile(
      "(?<attrname>" + AttrSpec.SPEC_PATTERN.pattern() + ")\s*=\s*(?<attrvalue>"
      + AttrValue.SPEC_PATTERN.pattern() + ")", Pattern.COMMENTS
  );

  /// parse an attribute spec into an attribute spec
  /// @param spec The textual representation of an attribute
  /// @return an attribute spec
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
