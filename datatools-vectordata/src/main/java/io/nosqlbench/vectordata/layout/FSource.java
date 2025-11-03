package io.nosqlbench.vectordata.layout;

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


import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// An FSource is a source of data, with an optional window.
public class FSource {
  /// the path to the source of the data
  private final String inpath;
  /// the window of the source of the data
  private final FWindow window;

  public FSource(String inpath, FWindow window) {
    this.inpath = inpath;
    this.window = window;
  }

  public String inpath() {
    return inpath;
  }

  public FWindow window() {
    return window;
  }

  /// Creates a new FSource with the specified path and window
  /// @param origin the path to the source of the data
  /// @param window the window of the source of the data
  public FSource(String origin, String window) {
    this(origin, FWindow.parse(window));
  }

  /// The pattern for parsing a source spec
  /// Supports window notation with:
  ///   - Brackets: file.ext[window] or file.ext(window)
  ///   - Colon: file.ext:window
  public static Pattern PATTERN = Pattern.compile("(?<source>.+?)(?<window>([\\[(:]).+)?$");

  private static URL parseUrl(String origin) {
    try {
      if (origin.startsWith("http")) {
        return new URL(origin);
      }
      if (origin.contains(".")) {
        return Path.of(origin).toUri().toURL();
      }
      return new URL("file://" + origin);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /// parses a source spec into a source and window
  /// @param spec the source spec to parse
  /// @return a source and window
  public static FSource parse(String spec) {
    Matcher matcher = PATTERN.matcher(spec);
    if (matcher.matches()) {
      String windowSpec = matcher.group("window");
      // Strip leading delimiter from window spec (e.g., ":1m" -> "1m", "(0..100)" -> "0..100)")
      if (windowSpec != null && !windowSpec.isEmpty()) {
        char firstChar = windowSpec.charAt(0);
        if (firstChar == ':' || firstChar == '[' || firstChar == '(') {
          windowSpec = windowSpec.substring(1);
        }
      }

      return new FSource(
          matcher.group("source"),
          FWindow.parse(windowSpec)
      );
    } else {
      throw new RuntimeException("Invalid data source spec: " + spec);
    }

  }

  /// Creates a new FSource from an object
  /// @param source the object to create a source from
  /// @return a new FSource
  public static FSource fromObject(Object source) {
    if (source instanceof String) {
      String s = (String) source;
      return parse(s);
    } else if (source instanceof Map<?, ?>) {
      Map<?, ?> m = (Map<?, ?>) source;
      return new FSource(
          (String) m.get("inpath"),
          FWindow.fromObject(m.get("window"))
      );
    }
    return null;
  }

  /// provide a raw data representation of this source
  /// @return the raw data representation of this source
  public String toData() {
    return inpath + ((window != FWindow.ALL) ? "(" + window.toData() + ")" : "");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    FSource that = (FSource) obj;
    return java.util.Objects.equals(inpath, that.inpath) && java.util.Objects.equals(window, that.window);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(inpath, window);
  }

  @Override
  public String toString() {
    return "FSource{inpath='" + inpath + "', window=" + window + '}';
  }
}
