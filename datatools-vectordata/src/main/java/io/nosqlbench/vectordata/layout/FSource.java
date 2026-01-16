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

/// An FSource is a source of data, with an optional window and type.
///
/// ## Source Types
///
/// Sources can be either file-backed (xvec) or generator-backed (virtdata):
/// - {@link SourceType#XVEC} - reads from .fvec/.ivec files
/// - {@link SourceType#VIRTDATA} - generates vectors from a model JSON file
///
/// ## Type Inference
///
/// When the type is not explicitly specified:
/// - Paths ending in `.json` are inferred as {@link SourceType#VIRTDATA}
/// - All other paths default to {@link SourceType#XVEC}
///
/// @see SourceType
public class FSource {
  /// the path to the source of the data
  private final String inpath;
  /// the window of the source of the data
  private final FWindow window;
  /// the type of the source (xvec or virtdata)
  private final SourceType type;

  public FSource(String inpath, FWindow window) {
    this(inpath, window, SourceType.inferFromPath(inpath));
  }

  public FSource(String inpath, FWindow window, SourceType type) {
    this.inpath = inpath;
    this.window = window;
    this.type = type != null ? type : SourceType.inferFromPath(inpath);
  }

  public String inpath() {
    return inpath;
  }

  public FWindow window() {
    return window;
  }

  /// Returns the source type.
  /// @return the source type (xvec or virtdata)
  public SourceType type() {
    return type;
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
      String inpath = (String) m.get("inpath");
      if (inpath == null) {
        inpath = (String) m.get("source"); // alternate key
      }
      if (inpath == null) {
        inpath = (String) m.get("path"); // alternate key
      }
      FWindow window = FWindow.fromObject(m.get("window"));

      // Parse type if specified, otherwise infer from path
      SourceType type = null;
      Object typeObj = m.get("type");
      if (typeObj instanceof String) {
        type = SourceType.fromString((String) typeObj);
      }
      if (type == null) {
        type = SourceType.inferFromPath(inpath);
      }

      return new FSource(inpath, window, type);
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
    return java.util.Objects.equals(inpath, that.inpath)
        && java.util.Objects.equals(window, that.window)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(inpath, window, type);
  }

  @Override
  public String toString() {
    return "FSource{type=" + type + ", inpath='" + inpath + "', window=" + window + '}';
  }
}
