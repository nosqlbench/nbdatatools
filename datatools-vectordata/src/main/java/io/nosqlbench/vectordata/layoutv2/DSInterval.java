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


import io.nosqlbench.vectordata.utils.UnitConversions;

import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/// Represents an interval with inclusive minimum and exclusive maximum boundaries.
/// Used to define ranges of data within a dataset.
public class DSInterval {
  /// The inclusive minimum boundary of the interval
  public long minIncl;
  /// The exclusive maximum boundary of the interval
  public long maxExcl;

  /// This pattern captures common variations of these interval syntax:
  /// ```
  ///(0..10)
  ///[0..10]
  ///(0-10)
  ///[0-10]
  ///(0→10) If font ligatures are on, this is actually (0 - > 10) without the space
  ///[0→10]
  ///```
  public final static Pattern PATTERN = Pattern.compile(
      "[(\\[]? \\s*\n" +
          "(?<start>\\d[\\d_]*\\w*) \\s*\n" +
          "((\\.\\.|-|→) \\s*\n" +
          "(?<end>\\d[\\d_]*\\w*))? \\s*\n" +
          "[)\\]]? \\s*\n", Pattern.COMMENTS | Pattern.DOTALL
  );

  /// Creates an interval with the specified boundaries.
  /// @param min
  ///     The inclusive minimum boundary
  /// @param max
  ///     The exclusive maximum boundary
  public DSInterval(long min, long max) {
    this.minIncl = min;
    this.maxExcl = max;
  }

  /// Creates an empty interval with uninitialized boundaries.
  public DSInterval() {
  }

  /// Create an interval object from another representative form.
  /// @param objdata
  ///     any object which can be converted, including the target type,
  ///     a map of fields, or a string.
  /// @return A {@link DSInterval} object
  public static DSInterval fromData(Object objdata) {
    if (objdata instanceof DSInterval) {
      return (DSInterval) objdata;
    } else if (objdata instanceof Map<?, ?>) {
      Map<?, ?> m = (Map<?, ?>) objdata;
      Double min = (Double) m.get("minIncl");
      Double max = (Double) m.get("maxExcl");
      return new DSInterval(min.longValue(), max.longValue());
    } else if (objdata instanceof String) {
      return parse((String) objdata);
    } else {
      throw new RuntimeException("invalid intervals format:" + objdata);
    }
  }

  /// parse a dataset interval
  /// @param interval
  ///     A string spec for interval, like '[10..1000)'
  /// @return A DSInterval object
  public static DSInterval parse(String interval) {
    if (interval == null || interval.isBlank()) {
      throw new RuntimeException("invalid intervals format:" + interval);
    }
    String trimmed = interval.trim();

    boolean hasWrapper = (trimmed.startsWith("[") || trimmed.startsWith("("))
        && (trimmed.endsWith("]") || trimmed.endsWith(")"));
    boolean startClosed = trimmed.startsWith("[");
    boolean endClosed = trimmed.endsWith("]");
    String inner = hasWrapper ? trimmed.substring(1, trimmed.length() - 1).trim() : trimmed;

    String[] parts = splitInterval(inner);
    if (parts.length == 1) {
      long single = parseCount(parts[0], interval);
      if (hasWrapper) {
        if (!startClosed || !endClosed) {
          throw new RuntimeException(
              "Single-value interval must be specified as [n]. Got: " + interval);
        }
        return new DSInterval(single, single + 1);
      }
      return new DSInterval(0, single);
    }
    if (parts.length != 2) {
      throw new RuntimeException(
          "invalid intervals format:" + interval + ", expected [start..end], [start,end), "
              + "start..end, or similar patterns.");
    }

    long start = parseCount(parts[0], interval);
    long end = parseCount(parts[1], interval);

    if (hasWrapper) {
      if (!startClosed) {
        start += 1;
      }
      if (endClosed) {
        end += 1;
      }
    } else {
      end += 1;
    }
    return new DSInterval(start, end);
  }

  private static String[] splitInterval(String inner) {
    if (inner.contains("..")) {
      return inner.split("\\.\\.", 2);
    }
    if (inner.contains("→")) {
      return inner.split("→", 2);
    }
    if (inner.contains("-")) {
      return inner.split("-", 2);
    }
    if (inner.contains(",")) {
      return inner.split(",", 2);
    }
    return new String[]{inner};
  }

  private static long parseCount(String value, String original) {
    String normalized = value.trim().replaceAll("_", "");
    return UnitConversions.longCountFor(normalized)
        .orElseThrow(() -> new RuntimeException("invalid intervals format:" + original));
  }


  /// Returns a string representation of this interval.
  /// @return A string representation of this interval
  @Override
  public String toString() {
    return new StringJoiner(", ", DSInterval.class.getSimpleName() + "[", "]").add(
        "minIncl=" + minIncl).toString();
  }

  /// Sets the exclusive maximum boundary of the interval.
  /// @param maxExcl
  ///     The exclusive maximum boundary
  public void setMaxExcl(long maxExcl) {
    this.maxExcl = maxExcl;
  }

  /// Sets the inclusive minimum boundary of the interval.
  /// @param minIncl
  ///     The inclusive minimum boundary
  public void setMinIncl(long minIncl) {
    this.minIncl = minIncl;
  }

  /// Gets the exclusive maximum boundary of the interval.
  /// @return The exclusive maximum boundary
  public long getMaxExcl() {
    return maxExcl;
  }

  /// Gets the inclusive minimum boundary of the interval.
  /// @return The inclusive minimum boundary
  public long getMinIncl() {
    return minIncl;
  }

  /// Compares this interval with another object for equality.
  /// @param o
  ///     The object to compare with
  /// @return True if the objects are equal, false otherwise
  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof DSInterval))
      return false;
    
    DSInterval interval = (DSInterval) o;
    return minIncl == interval.minIncl && maxExcl == interval.maxExcl;
  }

  /// Returns a hash code for this interval.
  /// @return A hash code value for this interval
  @Override
  public int hashCode() {
    int result = Long.hashCode(minIncl);
    result = 31 * result + Long.hashCode(maxExcl);
    return result;
  }

  /// Gets the startInclusive of the interval (alias for getMinIncl).
  /// @return The inclusive minimum boundary
  public long getStart() {
    return this.minIncl;
  }

  /// Gets the end of the interval (alias for getMaxExcl).
  /// @return The exclusive maximum boundary
  public long getEnd() {
    return this.maxExcl;
  }
}
