package io.nosqlbench.vectordata.layout.manifest;

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
    DSWindow window = new DSWindow();
    List<DSInterval> intervals = new ArrayList<>();
    if (data==null) {
      return ALL;
    } else if (data instanceof Map dataMap) {
      intervals.add(DSInterval.fromData(dataMap));
    } else if (data instanceof List<?> intervalObjs) {
      intervals = intervalObjs.stream().map(DSInterval::fromData).toList();
    }
    return new DSWindow(intervals);
  }
}
