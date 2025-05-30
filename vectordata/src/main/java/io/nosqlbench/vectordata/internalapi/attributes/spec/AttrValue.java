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
/// The format can support any of these examples/variants:
/// - `(String)astring` -> String
/// - `(int)234` -> int
/// - `(float)234` -> float
/// - `(byte)234` -> byte
/// - `12345678901234567890l` -> long
/// - `(long)12345678901234567890L` -> long
/// - `(String)12345678901234567890l` -> String
/// - `(int)12345678901234567890` -> ERROR
/// - `(int)foobarbaz` -? ERROR
///
/// If the type is specified, then it must be from the [ValueType] enum. This selects
/// the way the value is parsed. If the type is not specified, then it is inferred from the value.
/// This allows users to have easy type inference for values like "0.34", or to rely on literals
/// like "12345678901234567890L" for longs, or "(String)12345678901234567890l" for a string version.
/// @param type the type of attribute
/// @param literal the textual representation of the attribute
/// @param value the value of the attribute
/// @param <T> the Java value type
public record AttrValue<T>(
    ValueType type, String literal, T value
)
{

  /// a pattern to match attr specs
  @SuppressWarnings({"RegExpRepeatedSpace", "RegExpUnexpectedAnchor"})
  public static final Pattern SPEC_PATTERN = Pattern.compile(
      """
          (?:\\((?<typename>[a-zA-Z0-9_]+)\\))?    # Optional type hint (e.g., (String), (int))
          (?<literal>.+)                           # Value (required, captures everything after type hint or =)
          """, Pattern.COMMENTS
  );

  /// parse an attribute value spec into an attribute value
  /// @param spec The textual representation of an attribute
  /// @param <T> The type of the attribute value
  /// @return an attribute value
  public static <T> AttrValue<T> parse(String spec) {
    Matcher m = SPEC_PATTERN.matcher(spec);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid HDF5 value spec format: " + spec);
    }
    String typename = m.group("typename") != null ? m.group("typename").trim() : "";
    String literal = m.group("literal");

    ValueType type = typename.isEmpty() ? ValueType.fromLiteral(literal) :
        ValueType.valueOf(typename.toUpperCase());

    Object value = type.parse(literal);
    return new AttrValue<>(type, literal, (T) value);
  }

  /// an attribute value
  /// @param type the type of attribute
  /// @param literal the textual representation of the attribute
  /// @param value the value of the attribute
  public AttrValue {
    // Example of a simple validation:
    if (literal == null || literal.isEmpty()) {
      throw new IllegalArgumentException("value name cannot be null or empty.");
    }
  }
}
