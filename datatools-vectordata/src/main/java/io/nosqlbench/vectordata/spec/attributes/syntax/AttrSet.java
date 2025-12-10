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

/// This class captures a basic string specification for assigning an attribute to a dataset
/// element (group, view, file, etc.). The syntax is format-agnostic and works anywhere
/// attribute-style metadata is supported.
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
public class AttrSet {
    /// the attribute spec for the attribute to modify
    private final AttrSpec attrname;
    /// the attribute value to set
    private final AttrValue<?> attrvalue;
    
    public AttrSet(AttrSpec attrname, AttrValue<?> attrvalue) {
        this.attrname = attrname;
        this.attrvalue = attrvalue;
    }
    
    /// @return the attribute spec for the attribute to modify
    public AttrSpec attrname() {
        return attrname;
    }
    
    /// @return the attribute value to set
    public AttrValue<?> attrvalue() {
        return attrvalue;
    }
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor", "EscapedSpace"})
  private static final Pattern SPEC_PATTERN = Pattern.compile(
      "(?<attrname>" + AttrSpec.SPEC_PATTERN.pattern() + ")\\s*=\\s*(?<attrvalue>"
      + AttrValue.SPEC_PATTERN.pattern() + ")", Pattern.COMMENTS
  );

  /// parse an attribute spec into an attribute spec
  /// @param spec The textual representation of an attribute
  /// @return an attribute spec
  public static AttrSet parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid attribute spec format: " + spec);
    }
    AttrSpec attrname = AttrSpec.parse(m.group("attrname"));
    AttrValue<?> attrvalue = AttrValue.parse(m.group("attrvalue"));

    return new AttrSet(attrname, attrvalue);
  }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AttrSet attrSet = (AttrSet) obj;
        return attrname.equals(attrSet.attrname) &&
               attrvalue.equals(attrSet.attrvalue);
    }

    @Override
    public int hashCode() {
        int result = attrname.hashCode();
        result = 31 * result + attrvalue.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AttrSet{attrname=" + attrname + ", attrvalue=" + attrvalue + "}";
    }

}
