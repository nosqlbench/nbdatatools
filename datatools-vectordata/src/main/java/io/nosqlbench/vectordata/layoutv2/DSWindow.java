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


import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Represents a window of data as a collection of intervals.
/// Extends ArrayList to store multiple DSInterval objects.
public class DSWindow extends ArrayList<DSInterval> {
  /// Constant representing a window that includes all data
  public static final @NotNull DSWindow ALL = new DSWindow();

  /// Creates an empty window with no intervals.
  public DSWindow() {
  }

  /// Creates a window with the specified list of intervals.
  /// @param intervals The list of intervals to include in this window
  public DSWindow(List<DSInterval> intervals) {
    this.addAll(intervals);
  }

  /// Adds an interval to this window.
  /// @param interval The interval to add
  /// @return The added interval
  public DSInterval addInterval(DSInterval interval) {
    this.add(interval);
    return interval;
  }

  /// Creates a DSWindow from a list of data maps.
  /// @param data The list of data maps to create the window from
  /// @return A new DSWindow instance
  public static DSWindow fromData(Object data) {
    List<DSInterval> intervals = new ArrayList<>();
    if (data == null) {
      return ALL;
    } else if (data instanceof DSWindow) {
      return (DSWindow) data;
    } else if (data instanceof CharSequence) {
      String spec = data.toString().trim();
      if (spec.isEmpty()) {
        return ALL;
      }
      try {
        DSInterval interval = DSInterval.parse(spec);
        intervals.add(interval);
        return new DSWindow(intervals);
      } catch (RuntimeException ignored) {
        // fall through to multi-interval parsing
      }

      String innerSpec = spec;
      if ((innerSpec.startsWith("[") || innerSpec.startsWith("(")) &&
          (innerSpec.endsWith("]") || innerSpec.endsWith(")"))) {
        innerSpec = innerSpec.substring(1, innerSpec.length() - 1).trim();
      }

      List<String> parts = splitTopLevel(innerSpec);
      for (String part : parts) {
        String trimmed = part.trim();
        if (!trimmed.isEmpty()) {
          intervals.add(DSInterval.parse(trimmed));
        }
      }
      if (intervals.isEmpty()) {
        throw new RuntimeException("invalid window format:" + data);
      }
      return new DSWindow(intervals);
    } else if (data instanceof Number) {
      long end = ((Number) data).longValue();
      intervals.add(new DSInterval(0, end));
      return new DSWindow(intervals);
    } else if (data instanceof Map<?, ?>) {
      Map<?, ?> map = (Map<?, ?>) data;
      if (map.containsKey("intervals")) {
        return fromData(map.get("intervals"));
      }
      intervals.add(DSInterval.fromData(data));
      return new DSWindow(intervals);
    } else if (data instanceof List<?>) {
      List<?> intervalObjs = (List<?>) data;
      intervals = intervalObjs.stream().map(DSInterval::fromData).collect(java.util.stream.Collectors.toList());
      return new DSWindow(intervals);
    }
    throw new RuntimeException("invalid window format:" + data);
  }

  private static List<String> splitTopLevel(String spec) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int depth = 0;
    for (int i = 0; i < spec.length(); i++) {
      char c = spec.charAt(i);
      if (c == '[' || c == '(') {
        depth++;
      } else if (c == ']' || c == ')') {
        depth = Math.max(0, depth - 1);
      }
      if (c == ',' && depth == 0) {
        parts.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(c);
    }
    parts.add(current.toString());
    return parts;
  }
}
