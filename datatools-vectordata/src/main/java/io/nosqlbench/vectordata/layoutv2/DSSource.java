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


import io.nosqlbench.vectordata.layout.SourceType;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Represents a data source with a path, window, and type.
///
/// ## Source Types
///
/// Sources can be either file-backed (xvec) or generator-backed (virtdata):
/// - [SourceType#XVEC] - reads from .fvec/.ivec files
/// - [SourceType#VIRTDATA] - generates vectors from a model JSON file
///
/// ## Type Inference
///
/// When the type is not explicitly specified:
/// - Paths ending in `.json` are inferred as [SourceType#VIRTDATA]
/// - All other paths default to [SourceType#XVEC]
///
/// @see SourceType
public class DSSource {

  /// Pattern for parsing a source spec with optional window notation.
  /// Supports window notation with:
  ///   - Parentheses: file.ext(window)
  ///   - Brackets: file.ext[window]
  private static final Pattern SOURCE_SPEC_PATTERN = Pattern.compile(
      "^(?<path>.+?)(?<window>\\([^)]+\\)|\\[[^\\]]+\\])?$"
  );

  /// The path to the data source
  public String path;
  /// The window defining which portions of the data to include
  public DSWindow window;
  /// The type of source (xvec or virtdata)
  public SourceType type;

  /// Creates an empty data source with no path or window.
  public DSSource() {
  }

  /// Creates a data source with the specified path and a window that includes all data.
  /// The type is inferred from the path extension.
  /// @param path The path to the data source
  public DSSource(String path) {
    this.path = path;
    this.window = DSWindow.ALL;
    this.type = SourceType.inferFromPath(path);
  }

  /// Creates a data source with the specified path and window.
  /// The type is inferred from the path extension.
  /// @param path The path to the data source
  /// @param window The window defining which portions of the data to include
  public DSSource(String path, DSWindow window) {
    this.path = path;
    this.window = window;
    this.type = SourceType.inferFromPath(path);
  }

  /// Creates a data source with the specified path, window, and type.
  /// @param path The path to the data source
  /// @param window The window defining which portions of the data to include
  /// @param type The source type (xvec or virtdata)
  public DSSource(String path, DSWindow window, SourceType type) {
    this.path = path;
    this.window = window;
    this.type = type != null ? type : SourceType.inferFromPath(path);
  }

  /// Returns a string representation of this data source.
  /// @return A string representation of this data source
  @Override
  public String toString() {
    return new StringJoiner(", ", DSSource.class.getSimpleName() + "[", "]")
        .add("type=" + type)
        .add("path='" + path + "'")
        .add("window='" + window + "'")
        .toString();
  }

  /// Sets the window for this data source.
  /// @param window The window defining which portions of the data to include
  /// @return This data source for method chaining
  public DSSource setWindow(DSWindow window) {
    this.window = window;
    return this;
  }
  /// Sets the window for this data source using a list of intervals.
  /// @param intervals The list of intervals to include in the window
  /// @return The newly created window
  public DSWindow setWindow(List<DSInterval> intervals) {
    this.window = new DSWindow(intervals);
    return this.window;
  }

  /// Compares this data source with another object for equality.
  /// @param o The object to compare with
  /// @return True if the objects are equal, false otherwise
  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof DSSource))
      return false;

    DSSource dsSource = (DSSource) o;
    return Objects.equals(path, dsSource.path)
        && Objects.equals(window, dsSource.window)
        && type == dsSource.type;
  }

  /// Returns a hash code for this data source.
  /// @return A hash code value for this data source
  @Override
  public int hashCode() {
    int result = Objects.hashCode(path);
    result = 31 * result + Objects.hashCode(window);
    result = 31 * result + Objects.hashCode(type);
    return result;
  }

  /// Gets the path to the data source.
  /// @return The path to the data source
  public String getPath() {
    return this.path;
  }

  /// Gets the window defining which portions of the data to include.
  /// @return The window
  public DSWindow getWindow() {
    return this.window;
  }

  /// Gets the source type.
  /// @return The source type (xvec or virtdata)
  public SourceType getType() {
    return this.type;
  }

  /// Returns true if this is a virtdata (generator-backed) source.
  /// @return true if type is VIRTDATA
  public boolean isVirtdata() {
    return type == SourceType.VIRTDATA;
  }

  /// Returns true if this is an xvec (file-backed) source.
  /// @return true if type is XVEC
  public boolean isXvec() {
    return type == SourceType.XVEC;
  }

  /// Creates a DSSource from a map of data.
  ///
  /// When the data is a String, it is parsed to extract an optional window specifier.
  /// Supported formats:
  ///   - `path/to/file.fvec` - simple path with no window
  ///   - `path/to/file.fvec(0..1000)` - path with window in parentheses
  ///   - `path/to/file.fvec[0..1000]` - path with window in brackets
  ///
  /// @param data The map of data to create the source from
  /// @return A new DSSource instance
  public static DSSource fromData(Object data) {
    if (data == null) {
      return null;
    }
    if (data instanceof String) {
      return parseSourceSpec((String) data);
    } else if (data instanceof Path) {
      return new DSSource(((Path) data).toString());
    } else if (data instanceof DSSource) {
      return (DSSource) data;
    } else if (data instanceof Map<?, ?>) {
      Map<?, ?> m = (Map<?, ?>) data;
      String path = (String) m.get("path");
      if (path == null) {
        path = (String) m.get("source"); // alternate key
      }
      DSWindow window = DSWindow.fromData(m.get("window"));

      // Parse type if specified, otherwise infer from path
      SourceType type = null;
      Object typeObj = m.get("type");
      if (typeObj instanceof String) {
        type = SourceType.fromString((String) typeObj);
      }
      if (type == null) {
        type = SourceType.inferFromPath(path);
      }

      return new DSSource(path, window, type);
    } else {
      throw new RuntimeException("invalid source format:" + data);
    }
  }

  /// Parses a source spec string to extract path and optional window.
  ///
  /// @param spec The source spec string
  /// @return A new DSSource with parsed path and window
  private static DSSource parseSourceSpec(String spec) {
    if (spec == null || spec.isBlank()) {
      return new DSSource(spec);
    }

    Matcher matcher = SOURCE_SPEC_PATTERN.matcher(spec);
    if (matcher.matches()) {
      String path = matcher.group("path");
      String windowSpec = matcher.group("window");

      if (windowSpec != null && !windowSpec.isEmpty()) {
        // Strip outer delimiters: (...) -> ..., [...] -> ...
        String innerWindow = windowSpec;
        if (innerWindow.startsWith("(") && innerWindow.endsWith(")")) {
          innerWindow = innerWindow.substring(1, innerWindow.length() - 1);
        } else if (innerWindow.startsWith("[") && innerWindow.endsWith("]")) {
          innerWindow = innerWindow.substring(1, innerWindow.length() - 1);
        }

        DSWindow window = DSWindow.fromData(innerWindow);
        return new DSSource(path, window);
      }

      return new DSSource(path, DSWindow.ALL);
    }

    // If pattern doesn't match, treat entire string as path
    return new DSSource(spec);
  }
}
