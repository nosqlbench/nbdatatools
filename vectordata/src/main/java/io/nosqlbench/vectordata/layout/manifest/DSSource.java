package io.nosqlbench.vectordata.layout.manifest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/// Represents a data source with a path and a window defining which portions of the data to include.
public class DSSource {
  /// The path to the data source
  public String path;
  /// The window defining which portions of the data to include
  public DSWindow window;

  /// Creates an empty data source with no path or window.
  public DSSource() {
  }

  /// Creates a data source with the specified path and a window that includes all data.
  /// @param path The path to the data source
  public DSSource(String path) {
    this.path = path;
    this.window = DSWindow.ALL;
  }

  /// Creates a data source with the specified path and window.
  /// @param path The path to the data source
  /// @param window The window defining which portions of the data to include
  public DSSource(String path, DSWindow window) {
    this.path = path;
    this.window = window;
  }

  /// Returns a string representation of this data source.
  /// @return A string representation of this data source
  @Override
  public String toString() {
    return new StringJoiner(", ", DSSource.class.getSimpleName() + "[", "]").add(
        "path='" + path + "'").add("window='" + window + "'").toString();
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
    if (!(o instanceof DSSource dsSource))
      return false;

    return Objects.equals(path, dsSource.path) && Objects.equals(window, dsSource.window);
  }

  /// Returns a hash code for this data source.
  /// @return A hash code value for this data source
  @Override
  public int hashCode() {
    int result = Objects.hashCode(path);
    result = 31 * result + Objects.hashCode(window);
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

  /// Creates a DSSource from a map of data.
  /// @param data The map of data to create the source from
  /// @return A new DSSource instance
  public static DSSource fromData(Object data) {
    if (data == null) {
      return null;
    }
    if (data instanceof String s) {
      return new DSSource(s);
    } else if (data instanceof Path p) {
      return new DSSource(p.toString());
    } else if (data instanceof DSSource dsSource) {
      return dsSource;
    } else if (data instanceof Map<?, ?> m) {
      String path = (String) m.get("path");
      DSWindow window = DSWindow.fromData(m.get("window"));
      return new DSSource(path, window);
    } else {
      throw new RuntimeException("invalid source format:" + data);
    }
  }
}